//! Dynamic runtime layout state and viewport-width adaptive placement router.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::domain::ids::{SessionName, TabId};

/// Screen width threshold below which side-by-side splitting violates `DEFAULT_MIN_PANE_COLS` (80).
pub const NARROW_MAX_COLUMNS: u16 = 159;
/// Screen width threshold at or above which three comfortable columns fit in a single tab.
pub const ULTRAWIDE_MIN_COLUMNS: u16 = 220;
/// Active tiled task threshold in `agent:tasks` that triggers automatic stacking.
pub const TASKS_STACK_THRESHOLD: usize = 4;
/// Default hard minimum column width preserved per pane when splitting horizontally.
pub const DEFAULT_MIN_PANE_COLS: u16 = 80;
/// Default hard minimum row height preserved per pane when splitting vertically.
pub const DEFAULT_MIN_PANE_ROWS: u16 = 16;

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

/// Configurable hard minimum dimensions enforced before splitting the focused pane.
///
/// Setting either field to `0` disables that axis guard.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct LayoutConstraints {
    pub min_pane_cols: u16,
    pub min_pane_rows: u16,
}

impl Default for LayoutConstraints {
    fn default() -> Self {
        Self {
            min_pane_cols: DEFAULT_MIN_PANE_COLS,
            min_pane_rows: DEFAULT_MIN_PANE_ROWS,
        }
    }
}

/// Live dimensions of the currently focused tiled pane in the active tab.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct FocusedPaneGeometry {
    pub columns: u16,
    pub rows: u16,
}

/// Runtime layout projection synchronized from Zellij.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LayoutState {
    pub session: SessionName,
    pub active_tab_id: Option<TabId>,
    pub active_tab_name: Option<String>,
    pub managed_tabs: ManagedTabs,
    pub viewport: Option<ViewportSnapshot>,
    pub focused_pane: Option<FocusedPaneGeometry>,
    pub tab_modes: HashMap<TabId, TabMode>,
}

impl LayoutState {
    pub fn new(session: SessionName) -> Self {
        Self {
            session,
            active_tab_id: None,
            active_tab_name: None,
            managed_tabs: ManagedTabs::default(),
            viewport: None,
            focused_pane: None,
            tab_modes: HashMap::new(),
        }
    }

    /// Estimated column width of the focused pane after a horizontal `split_right`.
    pub fn estimated_col_if_split(&self) -> Option<u16> {
        if let Some(focused) = self.focused_pane {
            Some(focused.columns / 2)
        } else {
            self.viewport.map(ViewportSnapshot::estimated_col_if_split)
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

/// Reason a requested or auto-selected placement was downgraded to protect pane usability.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LayoutFallbackReason {
    MinWidthExceeded {
        requested: ResolvedPlacement,
        focused_cols: u16,
        min_cols: u16,
    },
    MinHeightExceeded {
        requested: ResolvedPlacement,
        focused_rows: u16,
        min_rows: u16,
    },
    NoRoomForNewPane {
        requested: ResolvedPlacement,
    },
    HeadlessFallback {
        requested: ResolvedPlacement,
    },
}

impl LayoutFallbackReason {
    pub fn format_hint(self, effective: ResolvedPlacement, tab_name: Option<&str>) -> String {
        let dest = match tab_name {
            Some(name) => format!("{} (tab '{name}')", effective.as_str()),
            None => effective.as_str().to_owned(),
        };
        match self {
            Self::MinWidthExceeded {
                requested,
                focused_cols,
                min_cols,
            } => format!(
                "Layout downgraded from '{}' to {dest}: splitting the focused pane ({focused_cols} cols) would leave {} cols (< min_pane_cols={min_cols}).",
                requested.as_str(),
                focused_cols / 2,
            ),
            Self::MinHeightExceeded {
                requested,
                focused_rows,
                min_rows,
            } => format!(
                "Layout downgraded from '{}' to {dest}: splitting the focused pane ({focused_rows} rows) would leave {} rows (< min_pane_rows={min_rows}).",
                requested.as_str(),
                focused_rows / 2,
            ),
            Self::NoRoomForNewPane { requested } => format!(
                "Layout downgraded from '{}' to {dest}: current tab had no room for a new split pane.",
                requested.as_str(),
            ),
            Self::HeadlessFallback { requested } => format!(
                "Layout downgraded from '{}' to {dest}: headless session fallback.",
                requested.as_str(),
            ),
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
    pub fallback_reason: Option<LayoutFallbackReason>,
}

/// Pure layout decision engine (no I/O).
#[derive(Debug, Clone, Copy)]
pub struct LayoutRouter;

impl LayoutRouter {
    /// Resolves a `PlacementRequest` into a concrete `PlacementPlan` based on live viewport
    /// width, focused pane geometry, minimum size constraints, entity kind, and active pane counts.
    pub fn plan(
        layout: &LayoutState,
        kind: EntityKind,
        request: PlacementRequest,
        active_tasks_in_tasks_tab: usize,
        active_services_in_services_tab: usize,
        constraints: LayoutConstraints,
    ) -> PlacementPlan {
        let tier = layout.viewport.map_or(LayoutTier::Wide, |v| v.tier);
        let tiled_in_active = layout.viewport.map_or(0, |v| v.tiled_panes_in_active_tab);

        let desired = match request {
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
                (LayoutTier::Ultrawide, EntityKind::Task) => {
                    // `tiled_in_active` counts all tiled panes in the active tab (including
                    // user-opened panes) so automatic splits never crowd the user's workspace.
                    if tiled_in_active <= 2 {
                        ResolvedPlacement::SplitRight
                    } else {
                        ResolvedPlacement::BackgroundTab
                    }
                }
                (LayoutTier::Ultrawide, EntityKind::Service) => ResolvedPlacement::Stacked,
            },
        };

        // Enforce hard minimum dimensions only when the focused tiled pane's live geometry is
        // known; when unknown (e.g. focus is on a floating pane or no client is attached), rely on
        // the `No room for a new pane` / headless fallback path instead of guessing from tab size.
        let focused_cols = layout.focused_pane.map(|g| g.columns);
        let focused_rows = layout.focused_pane.map(|g| g.rows);

        let downgrade_target = match kind {
            EntityKind::Repl => ResolvedPlacement::Floating,
            EntityKind::Task | EntityKind::Service => ResolvedPlacement::BackgroundTab,
        };

        let (placement, fallback_reason) = match desired {
            ResolvedPlacement::SplitRight
                if constraints.min_pane_cols > 0
                    && focused_cols.is_some_and(|cols| cols / 2 < constraints.min_pane_cols) =>
            {
                (
                    downgrade_target,
                    Some(LayoutFallbackReason::MinWidthExceeded {
                        requested: desired,
                        focused_cols: focused_cols.unwrap_or(0),
                        min_cols: constraints.min_pane_cols,
                    }),
                )
            }
            ResolvedPlacement::SplitDown
                if constraints.min_pane_rows > 0
                    && focused_rows.is_some_and(|rows| rows / 2 < constraints.min_pane_rows) =>
            {
                (
                    downgrade_target,
                    Some(LayoutFallbackReason::MinHeightExceeded {
                        requested: desired,
                        focused_rows: focused_rows.unwrap_or(0),
                        min_rows: constraints.min_pane_rows,
                    }),
                )
            }
            other => (other, None),
        };

        Self::finalize_plan(
            layout,
            kind,
            placement,
            active_tasks_in_tasks_tab,
            active_services_in_services_tab,
            fallback_reason,
        )
    }

    /// Builds a `PlacementPlan` for an explicit downgrade placement (e.g. after Zellij rejects a
    /// split with `No room for a new pane`).
    pub fn plan_fallback(
        layout: &LayoutState,
        kind: EntityKind,
        fallback_placement: ResolvedPlacement,
        active_tasks_in_tasks_tab: usize,
        active_services_in_services_tab: usize,
        reason: LayoutFallbackReason,
    ) -> PlacementPlan {
        Self::finalize_plan(
            layout,
            kind,
            fallback_placement,
            active_tasks_in_tasks_tab,
            active_services_in_services_tab,
            Some(reason),
        )
    }

    fn finalize_plan(
        layout: &LayoutState,
        kind: EntityKind,
        placement: ResolvedPlacement,
        active_tasks_in_tasks_tab: usize,
        active_services_in_services_tab: usize,
        fallback_reason: Option<LayoutFallbackReason>,
    ) -> PlacementPlan {
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
                    fallback_reason,
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
                fallback_reason,
            },
        }
    }
}
