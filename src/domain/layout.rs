//! Dynamic runtime layout state and viewport-width adaptive placement router.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::domain::ids::{SessionName, TabId};

/// Screen width threshold below which side-by-side splitting produces cramped columns.
pub const NARROW_MAX_COLUMNS: u16 = 139;
/// Screen width threshold at or above which three comfortable columns fit in a single tab.
pub const ULTRAWIDE_MIN_COLUMNS: u16 = 220;
/// Active tiled task threshold in `agent:tasks` that triggers automatic stacking.
pub const TASKS_STACK_THRESHOLD: usize = 4;

/// Managed background tab name for finite tasks.
pub const MANAGED_TASKS_TAB_NAME: &str = "agent:tasks";
/// Managed background tab name for long-lived services.
pub const MANAGED_SERVICES_TAB_NAME: &str = "agent:services";

/// Optional placement parameter accepted at the MCP tool boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlacementRequest {
    #[default]
    Auto,
    SplitRight,
    SplitDown,
    Floating,
    Stacked,
    BackgroundTab,
}

/// Concrete placement decision after resolving `PlacementRequest::Auto`.
///
/// Using a separate type ensures `Auto` never leaks into `StateStore` or the Zellij CLI adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResolvedPlacement {
    SplitRight,
    SplitDown,
    Floating,
    Stacked,
    BackgroundTab,
}

impl From<ResolvedPlacement> for PlacementRequest {
    fn from(resolved: ResolvedPlacement) -> Self {
        match resolved {
            ResolvedPlacement::SplitRight => Self::SplitRight,
            ResolvedPlacement::SplitDown => Self::SplitDown,
            ResolvedPlacement::Floating => Self::Floating,
            ResolvedPlacement::Stacked => Self::Stacked,
            ResolvedPlacement::BackgroundTab => Self::BackgroundTab,
        }
    }
}

impl ResolvedPlacement {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SplitRight => "split_right",
            Self::SplitDown => "split_down",
            Self::Floating => "floating",
            Self::Stacked => "stacked",
            Self::BackgroundTab => "background_tab",
        }
    }
}

/// Viewport width tier driving adaptive placement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LayoutTier {
    Narrow,
    Wide,
    Ultrawide,
}

impl LayoutTier {
    pub const fn from_columns(columns: u16) -> Self {
        if columns <= NARROW_MAX_COLUMNS {
            Self::Narrow
        } else if columns >= ULTRAWIDE_MIN_COLUMNS {
            Self::Ultrawide
        } else {
            Self::Wide
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Narrow => "narrow",
            Self::Wide => "wide",
            Self::Ultrawide => "ultrawide",
        }
    }
}

/// Live viewport metrics for the currently active Zellij tab.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ViewportSnapshot {
    pub columns: u16,
    pub rows: u16,
    pub tiled_panes_in_active_tab: u16,
    pub tier: LayoutTier,
}

impl ViewportSnapshot {
    pub const fn new(columns: u16, rows: u16, tiled_panes_in_active_tab: u16) -> Self {
        Self {
            columns,
            rows,
            tiled_panes_in_active_tab,
            tier: LayoutTier::from_columns(columns),
        }
    }

    pub const fn estimated_col_if_split(self) -> u16 {
        let next_count = self.tiled_panes_in_active_tab.saturating_add(1);
        match self.columns.checked_div(next_count) {
            Some(cols) => cols,
            None => self.columns,
        }
    }
}

/// Lazily provisioned background tab IDs owned by the MCP server.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ManagedTabs {
    pub tasks: Option<TabId>,
    pub services: Option<TabId>,
}

/// Known tiling mode for a tab.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TabMode {
    Tiled,
    Stacked,
}

/// Runtime layout projection synchronized from Zellij.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LayoutState {
    pub session: SessionName,
    pub active_tab_id: Option<TabId>,
    pub managed_tabs: ManagedTabs,
    pub viewport: Option<ViewportSnapshot>,
    pub tab_modes: HashMap<TabId, TabMode>,
}

impl LayoutState {
    pub fn new(session: SessionName) -> Self {
        Self {
            session,
            active_tab_id: None,
            managed_tabs: ManagedTabs::default(),
            viewport: None,
            tab_modes: HashMap::new(),
        }
    }
}

/// Entity category used by the layout router to choose adaptive defaults.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntityKind {
    Task,
    Service,
    Repl,
}

/// Target tab selection produced by `LayoutRouter`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TargetTab {
    ActiveTab(Option<TabId>),
    ManagedTab {
        kind: ManagedTabKind,
        existing: Option<TabId>,
    },
}

/// Which managed background tab to provision or reuse.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManagedTabKind {
    Tasks,
    Services,
}

impl ManagedTabKind {
    pub const fn tab_name(self) -> &'static str {
        match self {
            Self::Tasks => MANAGED_TASKS_TAB_NAME,
            Self::Services => MANAGED_SERVICES_TAB_NAME,
        }
    }
}

/// Full pure placement plan produced before invoking Zellij CLI.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlacementPlan {
    pub placement: ResolvedPlacement,
    pub target_tab: TargetTab,
    pub open_stacked: bool,
    pub trigger_tab_stack_swap: bool,
}

/// Pure layout decision engine (no I/O).
#[derive(Debug, Clone, Copy)]
pub struct LayoutRouter;

impl LayoutRouter {
    /// Resolves a `PlacementRequest` into a concrete `PlacementPlan` based on live viewport
    /// width, entity kind, and active pane counts.
    pub fn plan(
        layout: &LayoutState,
        kind: EntityKind,
        request: PlacementRequest,
        active_tasks_in_tasks_tab: usize,
        active_services_in_services_tab: usize,
    ) -> PlacementPlan {
        let tier = layout.viewport.map_or(LayoutTier::Wide, |v| v.tier);
        let tiled_in_active = layout.viewport.map_or(0, |v| v.tiled_panes_in_active_tab);

        let placement = match request {
            PlacementRequest::SplitRight => ResolvedPlacement::SplitRight,
            PlacementRequest::SplitDown => ResolvedPlacement::SplitDown,
            PlacementRequest::Floating => ResolvedPlacement::Floating,
            PlacementRequest::Stacked => ResolvedPlacement::Stacked,
            PlacementRequest::BackgroundTab => ResolvedPlacement::BackgroundTab,
            PlacementRequest::Auto => match (tier, kind) {
                (LayoutTier::Narrow, EntityKind::Repl) => ResolvedPlacement::Floating,
                (LayoutTier::Narrow, EntityKind::Task | EntityKind::Service) => {
                    ResolvedPlacement::BackgroundTab
                }
                (LayoutTier::Wide, EntityKind::Repl) => {
                    if tiled_in_active <= 1 {
                        ResolvedPlacement::SplitRight
                    } else {
                        ResolvedPlacement::Floating
                    }
                }
                (LayoutTier::Wide, EntityKind::Task) => {
                    if tiled_in_active <= 1 {
                        ResolvedPlacement::SplitRight
                    } else {
                        ResolvedPlacement::BackgroundTab
                    }
                }
                (LayoutTier::Wide, EntityKind::Service) => ResolvedPlacement::BackgroundTab,
                (LayoutTier::Ultrawide, EntityKind::Repl) => {
                    if tiled_in_active <= 2 {
                        ResolvedPlacement::SplitRight
                    } else {
                        ResolvedPlacement::Stacked
                    }
                }
                (LayoutTier::Ultrawide, EntityKind::Task) => ResolvedPlacement::SplitRight,
                (LayoutTier::Ultrawide, EntityKind::Service) => ResolvedPlacement::Stacked,
            },
        };

        match placement {
            ResolvedPlacement::BackgroundTab => {
                let (tab_kind, existing, open_stacked, trigger_tab_stack_swap) = match kind {
                    EntityKind::Service => (
                        ManagedTabKind::Services,
                        layout.managed_tabs.services,
                        active_services_in_services_tab >= 1,
                        false,
                    ),
                    EntityKind::Task | EntityKind::Repl => {
                        let already_stacked = layout
                            .managed_tabs
                            .tasks
                            .and_then(|id| layout.tab_modes.get(&id))
                            == Some(&TabMode::Stacked);
                        (
                            ManagedTabKind::Tasks,
                            layout.managed_tabs.tasks,
                            false,
                            !already_stacked
                                && active_tasks_in_tasks_tab.saturating_add(1)
                                    >= TASKS_STACK_THRESHOLD,
                        )
                    }
                };
                PlacementPlan {
                    placement,
                    target_tab: TargetTab::ManagedTab {
                        kind: tab_kind,
                        existing,
                    },
                    open_stacked,
                    trigger_tab_stack_swap,
                }
            }
            ResolvedPlacement::SplitRight
            | ResolvedPlacement::SplitDown
            | ResolvedPlacement::Floating
            | ResolvedPlacement::Stacked => PlacementPlan {
                placement,
                target_tab: TargetTab::ActiveTab(layout.active_tab_id),
                open_stacked: placement == ResolvedPlacement::Stacked,
                trigger_tab_stack_swap: false,
            },
        }
    }
}
