use std::marker::PhantomData;

use crate::Entity;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelationKind {
    HasOne,
    HasMany,
    ManyToMany,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OnReplace {
    KeepMissing,
    DeleteMissing,
    UnlinkMissing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CascadeAction {
    None,
    Delete,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelationMetadata {
    pub name: &'static str,
    pub target: &'static str,
    pub kind: RelationKind,
    pub foreign_key: Option<&'static str>,
    pub references: Option<&'static str>,
    pub join_table: Option<&'static str>,
    pub source_key: Option<&'static str>,
    pub target_key: Option<&'static str>,
    pub target_primary_key: Option<&'static str>,
    pub on_replace: OnReplace,
    pub cascade: CascadeAction,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Include<A: AggregateRoot> {
    pub name: &'static str,
    _marker: PhantomData<fn() -> A>,
}

impl<A: AggregateRoot> Include<A> {
    pub fn new(name: &'static str) -> Self {
        Self {
            name,
            _marker: PhantomData,
        }
    }
}

pub trait AggregateRoot: Entity {
    fn relations() -> &'static [RelationMetadata] {
        &[]
    }

    fn include(name: &'static str) -> Include<Self> {
        Include::new(name)
    }
}
