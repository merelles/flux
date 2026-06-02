use flux::{
    Entity, GenericFilter, OrderDirection, Page, PageRequest, ReadRepository, RepositoryError,
    Result, Uuid,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Product {
    pub product_oid: Uuid,
    pub name: String,
    pub price: i32,
}

impl Entity for Product {
    type Id = Uuid;

    fn id(&self) -> &Self::Id {
        &self.product_oid
    }

    fn has_id(&self) -> bool {
        true
    }
}

pub struct ProductReadRepository {
    products: Vec<Product>,
}

#[async_trait::async_trait]
impl ReadRepository<Product> for ProductReadRepository {
    async fn find_by_id(&self, id: &Uuid) -> Result<Product> {
        self.products
            .iter()
            .find(|product| product.id() == id)
            .cloned()
            .ok_or(RepositoryError::NotFound)
    }

    async fn find_page(&self, page: PageRequest<Uuid>) -> Result<Page<Product, Uuid>> {
        let limit = page.limit();
        let items = self.products.iter().take(limit as usize).cloned().collect();
        Ok(Page::new(
            items,
            limit,
            None,
            Some(self.products.len() as u64),
        ))
    }

    async fn find_page_with_filter(
        &self,
        _filter: GenericFilter<Product>,
        page: PageRequest<Uuid>,
    ) -> Result<Page<Product, Uuid>> {
        self.find_page(page).await
    }

    async fn exists(&self, id: &Uuid) -> Result<bool> {
        Ok(self.products.iter().any(|product| product.id() == id))
    }

    async fn count(&self) -> Result<u64> {
        Ok(self.products.len() as u64)
    }
}

fn main() {
    let _filter = GenericFilter::<Product>::new()
        .eq("name", "Keyboard")
        .gte("price", 100)
        .order_by("price", OrderDirection::Desc);

    let _page = PageRequest::<Uuid>::cursor(50, None);
}
