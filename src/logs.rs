//! Bounded log ring buffer, viewport diffing, ANSI stripping, and output truncation.

use std::collections::VecDeque;

use serde::{Deserialize, Serialize};

use crate::domain::ids::{CompiledPattern, LogSeq};

/// Maximum number of log lines retained in memory per managed pane.
pub const MAX_RING_BUFFER_LINES: usize = 2000;
/// Default number of leading lines preserved during head/tail truncation.
pub const DEFAULT_TRUNCATE_HEAD: usize = 20;
/// Default number of trailing lines preserved during head/tail truncation.
pub const DEFAULT_TRUNCATE_TAIL: usize = 80;

/// Single line stored in a pane's bounded ring buffer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogEntry {
    pub seq: LogSeq,
    pub line: String,
}

/// Slice returned by incremental or tailed log queries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogSlice {
    pub entries: Vec<LogEntry>,
    pub next_seq: LogSeq,
    pub dropped_before: Option<LogSeq>,
}

/// Fixed-capacity ring buffer with monotonically increasing sequence numbers.
#[derive(Debug, Clone)]
pub struct BoundedLogBuffer {
    capacity: usize,
    next_seq: LogSeq,
    entries: VecDeque<LogEntry>,
}

impl BoundedLogBuffer {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            next_seq: LogSeq::new(1),
            entries: VecDeque::with_capacity(capacity.min(MAX_RING_BUFFER_LINES)),
        }
    }

    pub fn push_line(&mut self, line: String) -> LogSeq {
        let seq = self.next_seq;
        self.next_seq = seq.next();
        if self.entries.len() == self.capacity {
            let _ = self.entries.pop_front();
        }
        self.entries.push_back(LogEntry { seq, line });
        seq
    }

    pub fn push_lines<I>(&mut self, lines: I) -> LogSeq
    where
        I: IntoIterator<Item = String>,
    {
        for line in lines {
            let _ = self.push_line(line);
        }
        self.next_seq
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn next_seq(&self) -> LogSeq {
        self.next_seq
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Reads lines with `seq >= since_seq` (inclusive cursor matching the `next_seq` returned by a
    /// previous call), filtered by `grep`, returning at most the last `tail_lines`.
    pub fn query(
        &self,
        since_seq: Option<LogSeq>,
        tail_lines: usize,
        grep: Option<&CompiledPattern>,
    ) -> LogSlice {
        let dropped_before = match (since_seq, self.entries.front()) {
            (Some(cursor), Some(oldest)) if cursor < oldest.seq => Some(oldest.seq),
            _ => None,
        };

        let mut matched: Vec<LogEntry> = self
            .entries
            .iter()
            .filter(|entry| since_seq.is_none_or(|cursor| entry.seq >= cursor))
            .filter(|entry| grep.is_none_or(|pat| pat.is_match(&entry.line)))
            .cloned()
            .collect();

        if matched.len() > tail_lines {
            let drain_count = matched.len() - tail_lines;
            let _ = matched.drain(..drain_count);
        }

        LogSlice {
            entries: matched,
            next_seq: self.next_seq,
            dropped_before,
        }
    }

    pub fn tail_preview(&self, max_lines: usize) -> String {
        if max_lines == 0 || self.entries.is_empty() {
            return String::new();
        }
        let skip = self.entries.len().saturating_sub(max_lines);
        self.entries
            .iter()
            .skip(skip)
            .map(|e| e.line.as_str())
            .collect::<Vec<_>>()
            .join("\n")
    }
}

impl Default for BoundedLogBuffer {
    fn default() -> Self {
        Self::with_capacity(MAX_RING_BUFFER_LINES)
    }
}

fn trim_trailing_blank_lines(lines: &[String]) -> &[String] {
    let mut end = lines.len();
    while end > 0 && lines[end - 1].trim().is_empty() {
        end -= 1;
    }
    &lines[..end]
}

/// Computes newly appended lines between two consecutive viewport snapshots.
pub fn diff_viewport(previous: &[String], current: &[String]) -> Vec<String> {
    let prev = trim_trailing_blank_lines(previous);
    let curr = trim_trailing_blank_lines(current);

    if curr.is_empty() {
        return Vec::new();
    }
    if prev.is_empty() {
        return curr.to_vec();
    }

    // Find the longest suffix of `prev` that equals a prefix of `curr`.
    let max_overlap = prev.len().min(curr.len());
    for overlap in (1..=max_overlap).rev() {
        if prev[prev.len() - overlap..] == curr[..overlap] {
            return curr[overlap..].to_vec();
        }
    }

    // Handle simultaneous scroll + in-place rewrite of the previous last line (e.g. a prompt line
    // updated with typed input) by checking suffix-prefix overlap against `prev[..prev.len()-1]`.
    if prev.len() >= 2 {
        let prev_stable = &prev[..prev.len() - 1];
        let max_stable = prev_stable.len().min(curr.len());
        for overlap in (1..=max_stable).rev() {
            if prev_stable[prev_stable.len() - overlap..] == curr[..overlap] {
                return curr[overlap..].to_vec();
            }
        }
    }

    // When a line is rewritten in place without scrolling, emit only the diverging tail.
    let common_prefix = prev
        .iter()
        .zip(curr.iter())
        .take_while(|(a, b)| a == b)
        .count();
    curr[common_prefix..].to_vec()
}

/// Strips ANSI CSI, OSC, DCS/APC/PM/SOS, and charset escape sequences from terminal output.
pub fn strip_ansi(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch != '\x1b' {
            out.push(ch);
            continue;
        }
        match chars.peek().copied() {
            Some('[') => {
                // CSI sequence: ESC [ ... <0x40..=0x7E>
                let _ = chars.next();
                for c in chars.by_ref() {
                    if ('@'..='~').contains(&c) {
                        break;
                    }
                }
            }
            Some(']') => {
                // OSC sequence: ESC ] ... (BEL or ESC \)
                let _ = chars.next();
                while let Some(c) = chars.next() {
                    if c == '\x07' {
                        break;
                    }
                    if c == '\x1b' && chars.peek() == Some(&'\\') {
                        let _ = chars.next();
                        break;
                    }
                }
            }
            Some('P' | 'X' | '^' | '_') => {
                // DCS / SOS / PM / APC string terminated by ST (ESC \)
                let _ = chars.next();
                while let Some(c) = chars.next() {
                    if c == '\x1b' && chars.peek() == Some(&'\\') {
                        let _ = chars.next();
                        break;
                    }
                }
            }
            Some('(' | ')' | '*' | '+') => {
                // 3-byte G0-G3 character set designation (e.g. `ESC ( B` from `tput sgr0`)
                let _ = chars.next();
                let _ = chars.next();
            }
            Some(_) => {
                // Two-byte escape sequence.
                let _ = chars.next();
            }
            None => break,
        }
    }

    out
}

/// Truncates a slice of lines to `head` leading + omission marker + `tail` trailing lines.
pub fn truncate_head_tail(lines: &[String], head: usize, tail: usize) -> String {
    if lines.len() <= head.saturating_add(tail) {
        return lines.join("\n");
    }
    let omitted = lines.len() - head - tail;
    let mut parts = Vec::with_capacity(head + tail + 1);
    parts.extend(lines[..head].iter().map(String::as_str));
    let marker = format!("... ({omitted} lines omitted) ...");
    parts.push(&marker);
    parts.extend(lines[lines.len() - tail..].iter().map(String::as_str));
    parts.join("\n")
}

/// Filters raw lines (e.g. from `dump-screen --full`) by optional regex and tail count.
pub fn filter_and_tail_lines(
    raw_text: &str,
    tail_lines: usize,
    grep: Option<&CompiledPattern>,
) -> Vec<String> {
    let cleaned = strip_ansi(raw_text);
    let mut lines: Vec<String> = cleaned
        .lines()
        .filter(|line| grep.is_none_or(|pat| pat.is_match(line)))
        .map(ToOwned::to_owned)
        .collect();

    if lines.len() > tail_lines {
        let drain_count = lines.len() - tail_lines;
        let _ = lines.drain(..drain_count);
    }
    lines
}
