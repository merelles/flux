use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::{
    parse_macro_input, punctuated::Punctuated, Data, DataEnum, DeriveInput, Expr, ExprLit, Field,
    Fields, GenericArgument, Ident, Lit, LitStr, Meta, Path, PathArguments, Type,
};

#[proc_macro_derive(Enum)]
pub fn enum_sql_derive(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    let variants = match input.data {
        Data::Enum(DataEnum { variants, .. }) => variants,
        _ => panic!("Enum can only be derived for enums"),
    };

    let variant_strings = variants
        .iter()
        .map(|variant| variant.ident.to_string().to_uppercase())
        .collect::<Vec<_>>();

    let to_sql_arms = variants
        .iter()
        .zip(variant_strings.iter())
        .map(|(variant, value)| {
            let ident = &variant.ident;
            quote! { Self::#ident => #value }
        });

    let from_sql_arms = variants
        .iter()
        .zip(variant_strings.iter())
        .map(|(variant, value)| {
            let ident = &variant.ident;
            quote! { #value => Ok(Self::#ident) }
        });

    quote! {
        impl ::tokio_postgres::types::ToSql for #name {
            fn to_sql(
                &self,
                _ty: &::tokio_postgres::types::Type,
                out: &mut ::tokio_postgres::types::private::BytesMut,
            ) -> Result<::tokio_postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
                let value = match self {
                    #( #to_sql_arms, )*
                };
                out.extend_from_slice(value.as_bytes());
                Ok(::tokio_postgres::types::IsNull::No)
            }

            fn accepts(ty: &::tokio_postgres::types::Type) -> bool {
                ty == &::tokio_postgres::types::Type::TEXT
            }

            ::tokio_postgres::types::to_sql_checked!();
        }

        impl<'a> ::tokio_postgres::types::FromSql<'a> for #name {
            fn from_sql(
                ty: &::tokio_postgres::types::Type,
                raw: &'a [u8],
            ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
                if !<Self as ::tokio_postgres::types::ToSql>::accepts(ty) {
                    return Err(format!("incompatible PostgreSQL type: {}", ty).into());
                }

                let value = std::str::from_utf8(raw)?;
                match value {
                    #( #from_sql_arms, )*
                    _ => Err(format!("invalid value for {}: {}", stringify!(#name), value).into()),
                }
            }

            fn accepts(ty: &::tokio_postgres::types::Type) -> bool {
                <Self as ::tokio_postgres::types::ToSql>::accepts(ty)
            }
        }
    }
    .into()
}

#[proc_macro_derive(Entity, attributes(primary_key, skip))]
pub fn entity_derive(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;
    let primary_key = primary_key_field(&input);
    let primary_ident = primary_key
        .ident
        .clone()
        .expect("primary key must be named");
    let primary_ty = &primary_key.ty;

    quote! {
        impl ::flux::Entity for #name {
            type Id = #primary_ty;

            fn id(&self) -> &Self::Id {
                &self.#primary_ident
            }

            fn has_id(&self) -> bool {
                true
            }
        }
    }
    .into()
}

#[proc_macro_derive(
    SqlEntity,
    attributes(table_name, primary_key, skip, has_one, has_many, many_to_many)
)]
pub fn sql_entity_derive(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;
    let table_name = container_string_attr(&input, "table_name")
        .unwrap_or_else(|| snake_case(&name.to_string()));
    let primary_key = primary_key_field(&input);
    let primary_ident = primary_key
        .ident
        .clone()
        .expect("primary key must be named");
    let primary_name = primary_ident.to_string();
    let fields = named_fields(&input);
    let persisted = fields
        .iter()
        .filter(|field| !is_skipped_or_relation(field))
        .collect::<Vec<_>>();

    let field_names = persisted
        .iter()
        .map(|field| field.ident.as_ref().unwrap().to_string())
        .collect::<Vec<_>>();
    let field_idents = persisted
        .iter()
        .map(|field| field.ident.as_ref().unwrap())
        .collect::<Vec<_>>();
    let update_idents = persisted
        .iter()
        .filter(|field| field.ident.as_ref().unwrap() != &primary_ident)
        .map(|field| field.ident.as_ref().unwrap())
        .collect::<Vec<_>>();

    let from_row_fields = fields.iter().map(|field| {
        let ident = field.ident.as_ref().unwrap();
        if is_skipped_or_relation(field) {
            quote! { #ident: ::core::default::Default::default() }
        } else {
            let column = ident.to_string();
            quote! {
                #ident: row.try_get(#column)
                    .map_err(|error| ::flux::RepositoryError::InvalidData(error.to_string()))?
            }
        }
    });

    quote! {
        impl ::flux_postgres::SqlEntity for #name {
            fn table_name() -> &'static str {
                #table_name
            }

            fn primary_key() -> &'static str {
                #primary_name
            }

            fn fields() -> &'static [&'static str] {
                &[#(#field_names),*]
            }

            fn from_row(row: ::tokio_postgres::Row) -> ::flux::Result<Self> {
                Ok(Self {
                    #( #from_row_fields, )*
                })
            }

            fn to_insert_params(&self) -> Vec<&(dyn ::tokio_postgres::types::ToSql + Sync)> {
                vec![
                    #( &self.#field_idents as &(dyn ::tokio_postgres::types::ToSql + Sync), )*
                ]
            }

            fn to_update_params(&self) -> Vec<&(dyn ::tokio_postgres::types::ToSql + Sync)> {
                vec![
                    #( &self.#update_idents as &(dyn ::tokio_postgres::types::ToSql + Sync), )*
                ]
            }

            fn primary_key_param(&self) -> &(dyn ::tokio_postgres::types::ToSql + Sync) {
                &self.#primary_ident
            }
        }
    }
    .into()
}

#[proc_macro_derive(
    MongoEntity,
    attributes(
        collection_name,
        id_field,
        primary_key,
        skip,
        has_one,
        has_many,
        many_to_many
    )
)]
pub fn mongo_entity_derive(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;
    let collection_name = container_string_attr(&input, "collection_name")
        .unwrap_or_else(|| snake_case(&name.to_string()));
    let id_field = container_string_attr(&input, "id_field").unwrap_or_else(|| "_id".to_string());
    let primary_key = primary_key_field(&input);
    let primary_ident = primary_key
        .ident
        .clone()
        .expect("primary key must be named");
    let fields = named_fields(&input);

    let from_document_fields = fields.iter().map(|field| {
        let ident = field.ident.as_ref().unwrap();
        let ty = &field.ty;
        if is_skipped_or_relation(field) {
            quote! { #ident: ::core::default::Default::default() }
        } else {
            let bson_field = if ident == &primary_ident {
                id_field.clone()
            } else {
                ident.to_string()
            };
            quote! {
                #ident: {
                    let value = document.remove(#bson_field).ok_or_else(|| {
                        ::flux::RepositoryError::InvalidData(format!(
                            "missing Mongo field {}",
                            #bson_field
                        ))
                    })?;
                    <#ty as ::flux_mongodb::MongoField>::from_bson(value)?
                }
            }
        }
    });

    let document_inserts = fields
        .iter()
        .filter(|field| !is_skipped_or_relation(field))
        .map(|field| {
            let ident = field.ident.as_ref().unwrap();
            let bson_field = if ident == &primary_ident {
                id_field.clone()
            } else {
                ident.to_string()
            };
            quote! {
                document.insert(
                    #bson_field,
                    ::flux_mongodb::MongoField::to_bson(&self.#ident)?,
                );
            }
        });

    quote! {
        impl ::flux_mongodb::MongoEntity for #name {
            fn collection_name() -> &'static str {
                #collection_name
            }

            fn id_field() -> &'static str {
                #id_field
            }

            fn from_document(mut document: ::mongodb::bson::Document) -> ::flux::Result<Self> {
                Ok(Self {
                    #( #from_document_fields, )*
                })
            }

            fn to_document(&self) -> ::flux::Result<::mongodb::bson::Document> {
                let mut document = ::mongodb::bson::Document::new();
                #( #document_inserts )*
                Ok(document)
            }
        }
    }
    .into()
}

#[proc_macro_derive(
    AggregateRoot,
    attributes(has_one, has_many, many_to_many, primary_key, skip, table_name)
)]
pub fn aggregate_root_derive(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;
    let relations = relation_fields(&input);
    let metadata = relations.iter().map(relation_metadata_tokens);
    let include_methods = relations.iter().map(|relation| {
        let field = &relation.field_ident;
        let name = &relation.name;
        quote! {
            pub fn #field() -> ::flux::Include<Self> {
                ::flux::Include::new(#name)
            }
        }
    });

    let postgres_impl = if has_derive_named(&input, "SqlEntity") {
        postgres_aggregate_impl(name, &relations)
    } else {
        quote! {}
    };

    quote! {
        impl ::flux::AggregateRoot for #name {
            fn relations() -> &'static [::flux::RelationMetadata] {
                &[
                    #( #metadata, )*
                ]
            }
        }

        impl #name {
            #( #include_methods )*
        }

        #postgres_impl
    }
    .into()
}

fn postgres_aggregate_impl(name: &Ident, relations: &[RelationField]) -> TokenStream2 {
    let load_relations = relations.iter().map(|relation| {
        let relation_name = &relation.name;
        let field = &relation.field_ident;
        let ty = &relation.target_ty;
        let source = source_expr(relation, quote! { aggregate });
        match relation.kind {
            RelationKindMacro::HasOne => quote! {
                if includes.iter().any(|include| include.name == #relation_name) {
                    let metadata = Self::relations()
                        .iter()
                        .find(|metadata| metadata.name == #relation_name)
                        .expect("relation metadata generated by flux-derive");
                    aggregate.#field = repository
                        .load_has_one::<#ty, _>(metadata, #source)
                        .await?;
                }
            },
            RelationKindMacro::HasMany => quote! {
                if includes.iter().any(|include| include.name == #relation_name) {
                    let metadata = Self::relations()
                        .iter()
                        .find(|metadata| metadata.name == #relation_name)
                        .expect("relation metadata generated by flux-derive");
                    aggregate.#field = repository
                        .load_has_many::<#ty, _>(metadata, #source)
                        .await?;
                }
            },
            RelationKindMacro::ManyToMany => quote! {
                if includes.iter().any(|include| include.name == #relation_name) {
                    let metadata = Self::relations()
                        .iter()
                        .find(|metadata| metadata.name == #relation_name)
                        .expect("relation metadata generated by flux-derive");
                    aggregate.#field = repository
                        .load_many_to_many::<#ty, _>(metadata, #source)
                        .await?;
                }
            },
        }
    });

    let insert_relations = relations.iter().map(|relation| {
        relation_save_tokens(
            relation,
            quote! { aggregate },
            quote! { ::flux::GraphSaveMode::AppendChildren },
        )
    });

    let update_relations = relations
        .iter()
        .map(|relation| relation_save_tokens(relation, quote! { aggregate }, quote! { mode }));

    let delete_relations = relations.iter().map(|relation| {
        let relation_name = &relation.name;
        let ty = &relation.target_ty;
        match relation.kind {
            RelationKindMacro::ManyToMany => quote! {
                {
                    let metadata = Self::relations()
                        .iter()
                        .find(|metadata| metadata.name == #relation_name)
                        .expect("relation metadata generated by flux-derive");
                    repository.delete_many_to_many_links(metadata, id).await?;
                }
            },
            RelationKindMacro::HasOne | RelationKindMacro::HasMany => quote! {
                {
                    let metadata = Self::relations()
                        .iter()
                        .find(|metadata| metadata.name == #relation_name)
                        .expect("relation metadata generated by flux-derive");
                    repository.delete_relation::<#ty, _>(metadata, id).await?;
                }
            },
        }
    });

    quote! {
        #[::flux_postgres::async_trait]
        impl ::flux_postgres::PostgresAggregate for #name {
            async fn load_relations(
                repository: &::flux_postgres::PostgresRepository<Self>,
                aggregate: &mut Self,
                includes: &[::flux::Include<Self>],
            ) -> ::flux::Result<()> {
                #( #load_relations )*
                Ok(())
            }

            async fn insert_relations(
                repository: &::flux_postgres::PostgresRepository<Self>,
                aggregate: &Self,
            ) -> ::flux::Result<()> {
                #( #insert_relations )*
                Ok(())
            }

            async fn update_relations(
                repository: &::flux_postgres::PostgresRepository<Self>,
                aggregate: &Self,
                mode: ::flux::GraphSaveMode,
            ) -> ::flux::Result<()> {
                #( #update_relations )*
                Ok(())
            }

            async fn delete_relations(
                repository: &::flux_postgres::PostgresRepository<Self>,
                id: &Self::Id,
            ) -> ::flux::Result<()> {
                #( #delete_relations )*
                Ok(())
            }
        }
    }
}

fn relation_save_tokens(
    relation: &RelationField,
    aggregate: TokenStream2,
    mode: TokenStream2,
) -> TokenStream2 {
    let relation_name = &relation.name;
    let field = &relation.field_ident;
    let ty = &relation.target_ty;
    let source = source_expr(relation, aggregate.clone());
    match relation.kind {
        RelationKindMacro::HasOne => quote! {
            {
                let metadata = Self::relations()
                    .iter()
                    .find(|metadata| metadata.name == #relation_name)
                    .expect("relation metadata generated by flux-derive");
                repository
                    .save_has_one::<#ty, _>(metadata, #aggregate.#field.as_ref(), #source, #mode)
                    .await?;
            }
        },
        RelationKindMacro::HasMany => quote! {
            {
                let metadata = Self::relations()
                    .iter()
                    .find(|metadata| metadata.name == #relation_name)
                    .expect("relation metadata generated by flux-derive");
                repository
                    .save_has_many::<#ty, _>(metadata, #aggregate.#field.as_slice(), #source, #mode)
                    .await?;
            }
        },
        RelationKindMacro::ManyToMany => quote! {
            {
                let metadata = Self::relations()
                    .iter()
                    .find(|metadata| metadata.name == #relation_name)
                    .expect("relation metadata generated by flux-derive");
                repository
                    .save_many_to_many::<#ty, _>(metadata, #aggregate.#field.as_slice(), #source, #mode)
                    .await?;
            }
        },
    }
}

fn source_expr(relation: &RelationField, aggregate: TokenStream2) -> TokenStream2 {
    if let Some(references) = &relation.references {
        let ident = format_ident!("{}", references);
        quote! { &#aggregate.#ident }
    } else {
        quote! { <Self as ::flux::Entity>::id(#aggregate) }
    }
}

fn relation_metadata_tokens(relation: &RelationField) -> TokenStream2 {
    let name = &relation.name;
    let target_ty = &relation.target_ty;
    let kind = match relation.kind {
        RelationKindMacro::HasOne => quote! { ::flux::RelationKind::HasOne },
        RelationKindMacro::HasMany => quote! { ::flux::RelationKind::HasMany },
        RelationKindMacro::ManyToMany => quote! { ::flux::RelationKind::ManyToMany },
    };
    let foreign_key = option_str(&relation.foreign_key);
    let references = option_str(&relation.references);
    let join_table = option_str(&relation.join_table);
    let source_key = option_str(&relation.source_key);
    let target_key = option_str(&relation.target_key);
    let target_primary_key = option_str(&relation.target_primary_key);
    let on_replace = match relation.on_replace.as_deref() {
        Some("delete_missing") => quote! { ::flux::OnReplace::DeleteMissing },
        Some("unlink_missing") => quote! { ::flux::OnReplace::UnlinkMissing },
        _ => quote! { ::flux::OnReplace::KeepMissing },
    };
    let cascade = if relation.cascade_delete {
        quote! { ::flux::CascadeAction::Delete }
    } else {
        quote! { ::flux::CascadeAction::None }
    };

    quote! {
        ::flux::RelationMetadata {
            name: #name,
            target: ::core::stringify!(#target_ty),
            kind: #kind,
            foreign_key: #foreign_key,
            references: #references,
            join_table: #join_table,
            source_key: #source_key,
            target_key: #target_key,
            target_primary_key: #target_primary_key,
            on_replace: #on_replace,
            cascade: #cascade,
        }
    }
}

fn option_str(value: &Option<String>) -> TokenStream2 {
    match value {
        Some(value) => quote! { Some(#value) },
        None => quote! { None },
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RelationKindMacro {
    HasOne,
    HasMany,
    ManyToMany,
}

#[derive(Clone)]
struct RelationField {
    name: String,
    field_ident: Ident,
    target_ty: Type,
    kind: RelationKindMacro,
    foreign_key: Option<String>,
    references: Option<String>,
    join_table: Option<String>,
    source_key: Option<String>,
    target_key: Option<String>,
    target_primary_key: Option<String>,
    on_replace: Option<String>,
    cascade_delete: bool,
}

fn relation_fields(input: &DeriveInput) -> Vec<RelationField> {
    named_fields(input)
        .iter()
        .filter_map(|field| {
            let ident = field.ident.clone().expect("relation field must be named");
            field.attrs.iter().find_map(|attr| {
                let kind = if attr.path().is_ident("has_one") {
                    RelationKindMacro::HasOne
                } else if attr.path().is_ident("has_many") {
                    RelationKindMacro::HasMany
                } else if attr.path().is_ident("many_to_many") {
                    RelationKindMacro::ManyToMany
                } else {
                    return None;
                };

                let args = relation_args(attr);
                let target_ty = match kind {
                    RelationKindMacro::HasOne => option_inner(&field.ty)
                        .unwrap_or_else(|| panic!("{} must be Option<T>", ident)),
                    RelationKindMacro::HasMany | RelationKindMacro::ManyToMany => {
                        vec_inner(&field.ty).unwrap_or_else(|| panic!("{} must be Vec<T>", ident))
                    }
                };

                Some(RelationField {
                    name: ident.to_string(),
                    field_ident: ident.clone(),
                    target_ty,
                    kind,
                    foreign_key: args.value("foreign_key"),
                    references: args.value("references"),
                    join_table: args.value("join_table"),
                    source_key: args.value("source_key"),
                    target_key: args.value("target_key"),
                    target_primary_key: args.value("target_primary_key"),
                    on_replace: args.value("on_replace"),
                    cascade_delete: args.flags.iter().any(|flag| flag == "cascade_delete"),
                })
            })
        })
        .collect()
}

#[derive(Default)]
struct RelationArgs {
    values: Vec<(String, String)>,
    flags: Vec<String>,
}

impl RelationArgs {
    fn value(&self, key: &str) -> Option<String> {
        self.values
            .iter()
            .find(|(name, _)| name == key)
            .map(|(_, value)| value.clone())
    }
}

fn relation_args(attr: &syn::Attribute) -> RelationArgs {
    let mut args = RelationArgs::default();
    attr.parse_nested_meta(|meta| {
        let key = meta
            .path
            .get_ident()
            .map(ToString::to_string)
            .unwrap_or_else(|| panic!("relation argument must be an identifier"));

        if meta.input.is_empty() {
            args.flags.push(key);
            return Ok(());
        }

        let value = meta.value()?;
        let lit: LitStr = value.parse()?;
        args.values.push((key, lit.value()));
        Ok(())
    })
    .expect("invalid relation attribute");
    args
}

fn primary_key_field(input: &DeriveInput) -> &Field {
    named_fields(input)
        .iter()
        .find(|field| has_attr(field, "primary_key"))
        .unwrap_or_else(|| panic!("No field marked with #[primary_key]"))
}

fn named_fields(input: &DeriveInput) -> &Punctuated<Field, syn::token::Comma> {
    match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => &fields.named,
            _ => panic!("Flux derives require named struct fields"),
        },
        _ => panic!("Flux derives can only be applied to structs"),
    }
}

fn is_skipped_or_relation(field: &Field) -> bool {
    has_attr(field, "skip")
        || has_attr(field, "has_one")
        || has_attr(field, "has_many")
        || has_attr(field, "many_to_many")
}

fn has_attr(field: &Field, name: &str) -> bool {
    field.attrs.iter().any(|attr| attr.path().is_ident(name))
}

fn container_string_attr(input: &DeriveInput, name: &str) -> Option<String> {
    input.attrs.iter().find_map(|attr| {
        if !attr.path().is_ident(name) {
            return None;
        }

        match &attr.meta {
            Meta::NameValue(value) => match &value.value {
                Expr::Lit(ExprLit {
                    lit: Lit::Str(value),
                    ..
                }) => Some(value.value()),
                _ => panic!("{name} must be a string literal"),
            },
            _ => panic!("{name} must use #[{name} = \"...\"]"),
        }
    })
}

fn vec_inner(ty: &Type) -> Option<Type> {
    path_inner_type(ty, "Vec")
}

fn option_inner(ty: &Type) -> Option<Type> {
    path_inner_type(ty, "Option")
}

fn path_inner_type(ty: &Type, wrapper: &str) -> Option<Type> {
    let Type::Path(path) = ty else {
        return None;
    };
    let segment = path.path.segments.last()?;
    if segment.ident != wrapper {
        return None;
    }
    let PathArguments::AngleBracketed(arguments) = &segment.arguments else {
        return None;
    };
    arguments.args.iter().find_map(|argument| match argument {
        GenericArgument::Type(ty) => Some(ty.clone()),
        _ => None,
    })
}

fn has_derive_named(input: &DeriveInput, name: &str) -> bool {
    input.attrs.iter().any(|attr| {
        if !attr.path().is_ident("derive") {
            return false;
        }

        attr.parse_args_with(Punctuated::<Path, syn::Token![,]>::parse_terminated)
            .map(|paths| {
                paths.iter().any(|path| {
                    path.segments
                        .last()
                        .is_some_and(|segment| segment.ident == name)
                })
            })
            .unwrap_or(false)
    })
}

fn snake_case(value: &str) -> String {
    let mut out = String::new();
    for (index, ch) in value.chars().enumerate() {
        if ch.is_ascii_uppercase() {
            if index > 0 {
                out.push('_');
            }
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push(ch);
        }
    }
    out
}
