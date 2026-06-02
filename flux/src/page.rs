use crate::EntityId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PageRequest<Id> {
    Offset { limit: u32, offset: u64 },
    Cursor { limit: u32, after: Option<Id> },
}

impl<Id> PageRequest<Id> {
    pub fn offset(limit: u32, offset: u64) -> Self {
        Self::Offset { limit, offset }
    }

    pub fn cursor(limit: u32, after: Option<Id>) -> Self {
        Self::Cursor { limit, after }
    }

    pub fn limit(&self) -> u32 {
        match self {
            Self::Offset { limit, .. } | Self::Cursor { limit, .. } => *limit,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Page<T, Id>
where
    Id: EntityId,
{
    pub items: Vec<T>,
    pub limit: u32,
    pub next_cursor: Option<Id>,
    pub total: Option<u64>,
}

impl<T, Id> Page<T, Id>
where
    Id: EntityId,
{
    pub fn new(items: Vec<T>, limit: u32, next_cursor: Option<Id>, total: Option<u64>) -> Self {
        Self {
            items,
            limit,
            next_cursor,
            total,
        }
    }

    pub fn empty(limit: u32) -> Self {
        Self {
            items: Vec::new(),
            limit,
            next_cursor: None,
            total: Some(0),
        }
    }
}
