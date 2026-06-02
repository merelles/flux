use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Data, DataEnum, DeriveInput, Fields, Ident, Type};

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

    let (primary_key_ident, primary_key_ty) = extract_primary_key(&input);

    let expanded = quote! {
        impl ::flux::Entity for #name {
            type Id = #primary_key_ty;

            fn id(&self) -> &Self::Id {
                &self.#primary_key_ident
            }

            fn has_id(&self) -> bool {
                true
            }
        }
    };

    expanded.into()
}

fn extract_primary_key(input: &DeriveInput) -> (Ident, Type) {
    if let Data::Struct(data) = &input.data {
        if let Fields::Named(fields) = &data.fields {
            for field in &fields.named {
                for attr in &field.attrs {
                    if attr.path().is_ident("primary_key") {
                        return (field.ident.clone().unwrap(), field.ty.clone());
                    }
                }
            }
        }
    }
    panic!("No field marked with #[primary_key]");
}

// TODO: Implement AggregateRoot derive macro
// For now, users must implement AggregateRoot trait manually
