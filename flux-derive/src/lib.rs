use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Attribute, Data, DataEnum, DeriveInput, Fields, Ident, Lit, Meta};

#[proc_macro_derive(Enum)]
pub fn enum_sql_derive(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    let variants = match input.data {
        Data::Enum(DataEnum { variants, .. }) => variants,
        _ => panic!("EnumSql só pode ser aplicado a enums"),
    };

    // Preparar listas para as variantes
    let variant_idents: Vec<&Ident> = variants.iter().map(|v| &v.ident).collect();
    let variant_strings: Vec<String> = variants
        .iter()
        .map(|v| v.ident.to_string().to_uppercase())
        .collect();

    // Gerar os braços do match
    let match_arms_to_sql = variant_idents
        .iter()
        .zip(variant_strings.iter())
        .map(|(ident, s)| {
            quote! {
                Self::#ident => #s
            }
        });

    let match_arms_from_sql =
        variant_idents
            .iter()
            .zip(variant_strings.iter())
            .map(|(ident, s)| {
                quote! {
                    #s => Ok(Self::#ident)
                }
            });

    let expanded = quote! {
        impl ::tokio_postgres::types::ToSql for #name {
            fn to_sql(
                &self,
                _ty: &::tokio_postgres::types::Type,
                out: &mut ::tokio_postgres::types::private::BytesMut,
            ) -> Result<::tokio_postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
                let s = match self {
                    #( #match_arms_to_sql, )*
                };
                out.extend_from_slice(s.as_bytes());
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
                    return Err(format!("Tipo PostgreSQL incompatível: {}", ty).into());
                }

                let value = std::str::from_utf8(raw)?;
                match value {
                    #( #match_arms_from_sql, )*
                    _ => Err(format!("Valor inválido para {}: {}", stringify!(#name), value).into()),
                }
            }

            fn accepts(ty: &::tokio_postgres::types::Type) -> bool {
                <Self as ::tokio_postgres::types::ToSql>::accepts(ty)
            }
        }
    };

    TokenStream::from(expanded)
}

#[proc_macro_derive(Entity, attributes(table_name, primary_key, skip))]
pub fn entity_derive(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    let table_name = extract_table_name(&input.attrs);
    let primary_key_ident = extract_primary_key(&input);
    let primary_key_str = primary_key_ident.to_string();

    let (field_names, field_idents, _field_types, skipped_field_idents, update_field_idents) =
        extract_fields(&input, &primary_key_ident);

    let expanded = quote! {
        impl ::flux::Entity for #name {
            fn table_name() -> &'static str { #table_name }
            fn primary_key() -> &'static str { #primary_key_str }
            fn fields() -> Vec<&'static str> { vec![#(#field_names),*] }

            fn from_row(row: ::tokio_postgres::Row) -> ::std::result::Result<Self, ::std::boxed::Box<dyn ::std::error::Error + ::std::marker::Send + ::std::marker::Sync>> {
                Ok(Self {
                    #( #field_idents: row.try_get(#field_names)?, )*
                    #( #skipped_field_idents: ::std::default::Default::default(), )*
                })
            }

            fn to_insert_params(&self) -> Vec<&(dyn ::tokio_postgres::types::ToSql + ::std::marker::Sync)> {
                vec![#( &self.#field_idents ),*]
            }

            fn to_update_params(&self) -> Vec<&(dyn ::tokio_postgres::types::ToSql + ::std::marker::Sync)> {
                vec![#( &self.#update_field_idents ),*]
            }

            fn primary_key_value(&self) -> &(dyn ::tokio_postgres::types::ToSql + ::std::marker::Sync) {
                &self.#primary_key_ident
            }

            fn has_id(&self) -> bool {
                true // TODO: Implement based on PK type
            }
        }
    };

    expanded.into()
}

fn extract_table_name(attrs: &[Attribute]) -> String {
    for attr in attrs {
        if attr.path().is_ident("table_name") {
            if let Meta::NameValue(meta) = &attr.meta {
                if let syn::Expr::Lit(expr_lit) = &meta.value {
                    if let Lit::Str(lit) = &expr_lit.lit {
                        return lit.value();
                    }
                }
            }
        }
    }
    panic!("Missing #[table_name = \"...\"] attribute");
}

fn extract_primary_key(input: &DeriveInput) -> Ident {
    if let Data::Struct(data) = &input.data {
        if let Fields::Named(fields) = &data.fields {
            for field in &fields.named {
                for attr in &field.attrs {
                    if attr.path().is_ident("primary_key") {
                        return field.ident.clone().unwrap();
                    }
                }
            }
        }
    }
    panic!("No field marked with #[primary_key]");
}

fn extract_fields(
    input: &DeriveInput,
    primary_key_ident: &Ident,
) -> (
    Vec<String>,
    Vec<syn::Ident>,
    Vec<String>,
    Vec<syn::Ident>,
    Vec<syn::Ident>,
) {
    let mut field_names = Vec::new();
    let mut field_idents = Vec::new();
    let mut field_types = Vec::new();
    let mut skipped_field_idents = Vec::new();
    let mut update_field_idents = Vec::new();

    if let Data::Struct(data) = &input.data {
        if let Fields::Named(fields) = &data.fields {
            for field in &fields.named {
                let mut skip = false;

                for attr in &field.attrs {
                    if attr.path().is_ident("skip") {
                        skip = true;
                        break;
                    }
                }

                if let Some(ident) = &field.ident {
                    if skip {
                        skipped_field_idents.push(ident.clone());
                        continue;
                    }

                    field_names.push(ident.to_string());
                    field_idents.push(ident.clone());
                    let type_str = quote::quote! { #field.ty }.to_string();
                    field_types.push(type_str);

                    // Add to update fields if not primary key
                    if ident != primary_key_ident {
                        update_field_idents.push(ident.clone());
                    }
                }
            }
        }
    }
    (
        field_names,
        field_idents,
        field_types,
        skipped_field_idents,
        update_field_idents,
    )
}

// TODO: Implement AggregateRoot derive macro
// For now, users must implement AggregateRoot trait manually
