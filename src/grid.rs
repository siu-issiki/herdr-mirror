// Cell grid + renderer for the pane wrapper.
//
// Frame wire format (verified against preview 2026-06-30): the frame ANSI is
// strictly per-cell — ESC[r;cH ESC[..m <char> — with a trailing cursor CUP and
// ?25h/l visibility. No scroll regions, no relative moves. So a cell grid plus
// this small parser is a complete decoder; no VT emulator needed.

use std::fmt::Write as _;
use std::rc::Rc;
use unicode_width::UnicodeWidthChar;

#[derive(Clone, PartialEq)]
pub struct Cell {
    /// Rc: runs of cells share one SGR allocation
    pub sgr: Rc<str>,
    pub ch: char,
}

#[derive(Default)]
pub struct Grid {
    pub rows: Vec<Vec<Option<Cell>>>,
    pub width: usize,
    pub height: usize,
    pub cursor_row: usize,
    pub cursor_col: usize,
    pub cursor_visible: bool,
    /// 0-based last row with non-blank content
    pub content_bottom: usize,
    /// reused per-frame decode buffer (frames arrive many times a second)
    scratch: Vec<char>,
}

impl Grid {
    pub fn new() -> Grid {
        Grid { cursor_visible: true, ..Default::default() }
    }

    pub fn resize(&mut self, width: usize, height: usize) {
        if width == self.width && height == self.height {
            return;
        }
        self.width = width;
        self.height = height;
        self.clear();
    }

    pub fn clear(&mut self) {
        self.rows = vec![vec![None; self.width]; self.height];
        self.content_bottom = 0;
    }

    pub fn apply(&mut self, ansi: &str) {
        let mut chars = std::mem::take(&mut self.scratch);
        chars.clear();
        chars.extend(ansi.chars());
        let mut row = 0usize;
        let mut col = 0usize;
        let mut sgr: Rc<str> = Rc::from("");
        let mut i = 0usize;
        while i < chars.len() {
            if chars[i] == '\x1b' {
                if let Some((params, final_ch, len)) = parse_csi(&chars[i..]) {
                    match final_ch {
                        'H' => {
                            let mut it = params.split(';').map(|n| n.parse::<usize>().unwrap_or(1).max(1));
                            row = it.next().unwrap_or(1) - 1;
                            col = it.next().unwrap_or(1) - 1;
                        }
                        'm' => {
                            sgr = Rc::from(chars[i..i + len].iter().collect::<String>());
                        }
                        'J' => self.clear(),
                        'h' | 'l' if params == "?25" => self.cursor_visible = final_ch == 'h',
                        _ => {}
                    }
                    i += len;
                    continue;
                }
                if let Some(len) = parse_osc(&chars[i..]) {
                    i += len;
                    continue;
                }
                i += 2; // two-byte escape (charset selection etc.)
                continue;
            }
            let ch = chars[i];
            if ch >= ' ' || ch == '\t' {
                let ch = if ch == '\t' { ' ' } else { ch };
                let w = UnicodeWidthChar::width(ch).unwrap_or(1).max(1);
                if row < self.height && col < self.width {
                    self.rows[row][col] = Some(Cell { sgr: sgr.clone(), ch });
                    // wide char owns the next cell too: clear it so a stale narrow
                    // char can't survive a delta that only rewrites the left cell
                    if w == 2 && col + 1 < self.width {
                        self.rows[row][col + 1] = None;
                    }
                }
                col += w;
            }
            i += 1;
        }
        self.scratch = chars;
        // the scan position after the last CUP is the cursor: the frame ends
        // with an explicit cursor CUP followed only by visibility toggles
        self.cursor_row = row;
        self.cursor_col = col;
        // recompute (not just grow): a delta frame can erase content with
        // spaces, and a stale bottom would anchor the window onto blank rows
        self.content_bottom = self
            .rows
            .iter()
            .rposition(|cells| cells.iter().any(|c| c.as_ref().is_some_and(|c| c.ch != ' ')))
            .unwrap_or(0);
    }

    pub fn text_lines(&self) -> Vec<String> {
        self.rows
            .iter()
            .map(|cells| {
                cells
                    .iter()
                    .map(|c| c.as_ref().map(|c| c.ch).unwrap_or(' '))
                    .collect::<String>()
                    .trim_end()
                    .to_string()
            })
            .collect()
    }

    /// Extract the text of a linear (row-major) selection `[start, end]`, both in
    /// grid coords with `start <= end` (see [`normalize_selection`]). Column
    /// ranges follow ordinary terminal selection: the first row runs from its
    /// column to the line end, whole middle rows, and the last row up to its
    /// column; a single-row selection is just the column span. Wide-char spacer
    /// cells (the empty cell trailing a width-2 char) are dropped so a CJK glyph
    /// isn't doubled, each row is `trim_end`ed, and rows join with `\n`.
    pub fn selection_text(&self, start: (usize, usize), end: (usize, usize)) -> String {
        let (sr, sc) = start;
        let (er, ec) = end;
        let last = self.rows.len().saturating_sub(1);
        let er = er.min(last);
        let mut lines: Vec<String> = Vec::new();
        let mut row = sr;
        while row <= er {
            let (c0, c1) = if sr == er {
                (sc, ec)
            } else if row == sr {
                (sc, usize::MAX)
            } else if row == er {
                (0, ec)
            } else {
                (0, usize::MAX)
            };
            lines.push(self.row_text(row, c0, c1));
            row += 1;
        }
        lines.join("\n")
    }

    /// Collect the characters of one row over the inclusive column range
    /// `c0..=c1` (c1 saturates to the row's last cell). A wide-char spacer (a
    /// `None` cell whose left neighbour is a width-2 char) is skipped; any other
    /// `None` is a blank space. The result is `trim_end`ed.
    fn row_text(&self, row: usize, c0: usize, c1: usize) -> String {
        let Some(cells) = self.rows.get(row) else { return String::new() };
        let hi = c1.min(cells.len().saturating_sub(1));
        let mut s = String::new();
        let mut col = c0;
        while col <= hi {
            match cells.get(col).and_then(|c| c.as_ref()) {
                Some(cell) => s.push(cell.ch),
                None => {
                    let is_spacer = col > 0
                        && cells[col - 1]
                            .as_ref()
                            .is_some_and(|c| UnicodeWidthChar::width(c.ch).unwrap_or(1) >= 2);
                    if !is_spacer {
                        s.push(' ');
                    }
                }
            }
            col += 1;
        }
        s.trim_end().to_string()
    }
}

/// Grid row shown at the top of a bottom-anchored `out_rows`-tall window — the
/// same offset [`Renderer::paint`] uses, so mouse coords can be mapped to grid
/// cells consistently.
pub fn window_offset(grid: &Grid, out_rows: usize) -> usize {
    let bottom = grid.content_bottom.max(grid.cursor_row);
    (bottom + 1).saturating_sub(out_rows)
}

/// Order an (anchor, current) pair into linear (row-major) `(start, end)` so
/// `start <= end` regardless of drag direction.
pub fn normalize_selection(
    a: (usize, usize),
    b: (usize, usize),
) -> ((usize, usize), (usize, usize)) {
    if a <= b {
        (a, b)
    } else {
        (b, a)
    }
}

/// Is grid cell `(row, col)` inside the linear selection `sel` = (start, end),
/// start <= end? Middle rows select their whole width; the first/last row are
/// bounded by the anchor/current columns.
fn in_selection(row: usize, col: usize, sel: ((usize, usize), (usize, usize))) -> bool {
    let ((sr, sc), (er, ec)) = sel;
    if row < sr || row > er {
        return false;
    }
    let lo = if row == sr { sc } else { 0 };
    let hi = if row == er { ec } else { usize::MAX };
    col >= lo && col <= hi
}

/// CSI: ESC [ <params: 0-9;:?> <final: alpha>. Returns (params, final, char len).
fn parse_csi(chars: &[char]) -> Option<(String, char, usize)> {
    if chars.len() < 3 || chars[0] != '\x1b' || chars[1] != '[' {
        return None;
    }
    let mut params = String::new();
    for (idx, &c) in chars.iter().enumerate().skip(2).take(62) {
        if c.is_ascii_digit() || c == ';' || c == ':' || c == '?' {
            params.push(c);
        } else if c.is_ascii_alphabetic() {
            return Some((params, c, idx + 1));
        } else {
            return None;
        }
    }
    None
}

/// OSC: ESC ] … (BEL | ESC \). Returns char len.
fn parse_osc(chars: &[char]) -> Option<usize> {
    if chars.len() < 2 || chars[0] != '\x1b' || chars[1] != ']' {
        return None;
    }
    let mut i = 2;
    while i < chars.len() {
        match chars[i] {
            '\x07' => return Some(i + 1),
            '\x1b' if chars.get(i + 1) == Some(&'\\') => return Some(i + 2),
            '\x1b' => return None,
            _ => i += 1,
        }
    }
    None
}

// ---------------------------------------------------------------------------
// renderer: paints a window of the grid onto the local terminal

#[derive(Default)]
pub struct Renderer {
    last_rows: Vec<Option<String>>,
    status_text: String,
    /// active local drag-selection highlight, in grid coords `(start, end)` with
    /// start <= end; painted cells inside it get reverse video. Changing it makes
    /// the affected rows' painted strings differ, so paint's diff repaints them.
    selection: Option<((usize, usize), (usize, usize))>,
}

impl Renderer {
    pub fn new() -> Renderer {
        Renderer::default()
    }

    pub fn invalidate(&mut self) {
        self.last_rows.clear();
    }

    pub fn status(&mut self, text: &str) {
        self.status_text = text.to_string();
        self.last_rows.pop(); // force bottom row repaint
    }

    /// Set (or clear) the drag-selection highlight range, in grid coords.
    pub fn set_selection(&mut self, sel: Option<((usize, usize), (usize, usize))>) {
        self.selection = sel;
    }

    /// Build the ANSI to paint the grid into an out_cols × out_rows terminal.
    /// Bottom-anchored window: agent TUIs live at the bottom of the screen.
    pub fn paint(&mut self, grid: &Grid, out_cols: usize, out_rows: usize) -> String {
        let offset_r = window_offset(grid, out_rows);
        let mut out = String::from("\x1b[?2026h\x1b[?25l");
        // paint every local row (missing rows blank-fill), or the pane stays
        // blank before the first frame and the status row is unreachable
        let row_count = out_rows;
        if self.last_rows.len() < row_count {
            self.last_rows.resize(row_count, None);
        }
        for r in 0..row_count {
            let empty = Vec::new();
            let cells = grid.rows.get(r + offset_r).unwrap_or(&empty);
            let mut line = String::new();
            let mut prev_sgr: Option<&str> = None;
            let mut prev_rev = false;
            let grow = r + offset_r;
            let limit = out_cols.min(grid.width);
            let mut c = 0;
            while c < limit {
                let cell = cells.get(c).and_then(|c| c.as_ref());
                let sgr = cell.map(|c| &*c.sgr).unwrap_or("\x1b[0m");
                let rev = self.selection.is_some_and(|sel| in_selection(grow, c, sel));
                if prev_sgr != Some(sgr) || rev != prev_rev {
                    // an SGR run doesn't reset reverse video on its own, so cancel
                    // it explicitly when leaving a selected run
                    if prev_rev && !rev {
                        line.push_str("\x1b[27m");
                    }
                    line.push_str(if sgr.is_empty() { "\x1b[0m" } else { sgr });
                    if rev {
                        line.push_str("\x1b[7m");
                    }
                    prev_sgr = Some(sgr);
                    prev_rev = rev;
                }
                let ch = cell.map(|c| c.ch).unwrap_or(' ');
                let w = UnicodeWidthChar::width(ch).unwrap_or(1).max(1);
                if w == 2 && c + 1 >= limit {
                    // a wide char at the right edge would overflow the pane
                    line.push(' ');
                    c += 1;
                    continue;
                }
                line.push(ch);
                c += w;
            }
            let is_status_row = r == out_rows - 1 && !self.status_text.is_empty();
            let painted = if is_status_row {
                format!("\x1b[0;7m {} \x1b[0m\x1b[K", self.status_text)
            } else {
                format!("{line}\x1b[0m\x1b[K")
            };
            if self.last_rows.get(r).map(|p| p.as_deref()) != Some(Some(painted.as_str())) {
                let _ = write!(out, "\x1b[{};1H", r + 1);
                out.push_str(&painted);
                self.last_rows[r] = Some(painted);
            }
        }
        let cr = grid.cursor_row as isize - offset_r as isize;
        if grid.cursor_visible && cr >= 0 && (cr as usize) < out_rows && self.status_text.is_empty() {
            let _ = write!(out, "\x1b[{};{}H\x1b[?25h", cr + 1, grid.cursor_col.min(out_cols.saturating_sub(1)) + 1);
        }
        out.push_str("\x1b[?2026l");
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_per_cell_frame() {
        let mut g = Grid::new();
        g.resize(10, 4);
        g.apply("\x1b[1;1H\x1b[0mhi\x1b[3;2H\x1b[31mX\x1b[2;1H\x1b[?25h");
        assert_eq!(g.text_lines(), vec!["hi", "", " X", ""]);
        assert_eq!(g.content_bottom, 2);
        assert_eq!((g.cursor_row, g.cursor_col), (1, 0));
        assert!(g.cursor_visible);
        assert_eq!(&*g.rows[2][1].as_ref().unwrap().sgr, "\x1b[31m");
    }

    #[test]
    fn clear_and_visibility() {
        let mut g = Grid::new();
        g.resize(4, 2);
        g.apply("\x1b[1;1Habcd\x1b[2;1Hwxyz");
        assert_eq!(g.content_bottom, 1);
        g.apply("\x1b[2J\x1b[?25l");
        assert_eq!(g.text_lines(), vec!["", ""]);
        assert!(!g.cursor_visible);
    }

    #[test]
    fn skips_osc_and_tabs() {
        let mut g = Grid::new();
        g.resize(8, 1);
        g.apply("\x1b]0;title\x07\x1b[1;1Ha\tb");
        assert_eq!(g.text_lines(), vec!["a b"]);
    }

    #[test]
    fn content_bottom_shrinks_when_delta_erases() {
        let mut g = Grid::new();
        g.resize(6, 8);
        g.apply("\x1b[1;1Htop\x1b[7;1Hbottom");
        assert_eq!(g.content_bottom, 6);
        // delta frame erases the bottom content with spaces
        g.apply("\x1b[7;1H      ");
        assert_eq!(g.content_bottom, 0);
    }

    #[test]
    fn status_paints_on_empty_grid() {
        // before the first frame the grid is 0x0 — status must still render
        let g = Grid::new();
        let mut r = Renderer::new();
        r.status("reconnecting in 5s");
        let out = r.paint(&g, 80, 24);
        assert!(out.contains("reconnecting in 5s"));
    }

    #[test]
    fn renderer_bottom_anchors_and_status() {
        let mut g = Grid::new();
        g.resize(5, 10);
        g.apply("\x1b[10;1Hlast"); // content at the bottom row of a tall grid
        let mut r = Renderer::new();
        let out = r.paint(&g, 5, 3);
        // window shows rows 8..10 → "last" lands on the visible last row
        assert!(out.contains("last"));
        r.status("HELLO");
        let out2 = r.paint(&g, 5, 3);
        assert!(out2.contains("HELLO"));
        // unchanged rows are not repainted
        let out3 = r.paint(&g, 5, 3);
        assert!(!out3.contains("last"));
    }

    #[test]
    fn wide_chars_advance_two_cells() {
        let mut g = Grid::new();
        g.resize(10, 2);
        // wire format: wide char emitted once, next CUP jumps 2 columns
        g.apply("\x1b[1;1H\x1b[0mあ\x1b[1;3Hい\x1b[1;5Hx");
        assert_eq!(g.rows[0][0].as_ref().unwrap().ch, 'あ');
        assert!(g.rows[0][1].is_none());
        assert_eq!(g.rows[0][2].as_ref().unwrap().ch, 'い');
        assert_eq!(g.rows[0][4].as_ref().unwrap().ch, 'x');
        // run without CUP: col must advance by display width
        g.apply("\x1b[2;1Hあい");
        assert_eq!(g.rows[1][2].as_ref().unwrap().ch, 'い');
        assert_eq!((g.cursor_row, g.cursor_col), (1, 4));
    }

    #[test]
    fn renderer_skips_wide_spacer_cells() {
        let mut g = Grid::new();
        g.resize(6, 1);
        g.apply("\x1b[1;1H\x1b[0mあ\x1b[1;3Hい\x1b[1;5Hx");
        let mut r = Renderer::new();
        let out = r.paint(&g, 6, 1);
        // spacer cells must not be painted: terminal column stays aligned
        assert!(out.contains("あいx"));
    }

    #[test]
    fn wide_char_overwrite_clears_spacer() {
        let mut g = Grid::new();
        g.resize(6, 1);
        g.apply("\x1b[1;1Hab");
        g.apply("\x1b[1;1Hあ");
        assert_eq!(g.rows[0][0].as_ref().unwrap().ch, 'あ');
        assert!(g.rows[0][1].is_none());
    }

    #[test]
    fn normalize_selection_orders_endpoints() {
        // same row, current left of anchor
        assert_eq!(normalize_selection((0, 5), (0, 2)), ((0, 2), (0, 5)));
        // anchor below current: swap so start is the earlier row
        assert_eq!(normalize_selection((3, 1), (1, 8)), ((1, 8), (3, 1)));
        // already ordered
        assert_eq!(normalize_selection((1, 2), (4, 0)), ((1, 2), (4, 0)));
    }

    #[test]
    fn selection_text_single_row_column_span() {
        let mut g = Grid::new();
        g.resize(10, 2);
        g.apply("\x1b[1;1Hhello world");
        // columns 2..=6 → "llo w"
        assert_eq!(g.selection_text((0, 2), (0, 6)), "llo w");
    }

    #[test]
    fn selection_text_multi_row_linear() {
        let mut g = Grid::new();
        g.resize(8, 3);
        g.apply("\x1b[1;1Habcdef\x1b[2;1Hmiddle\x1b[3;1Hxyz");
        // start mid first row, whole middle row, up to col on last row
        let text = g.selection_text((0, 3), (2, 1));
        assert_eq!(text, "def\nmiddle\nxy");
    }

    #[test]
    fn selection_text_excludes_wide_spacer() {
        let mut g = Grid::new();
        g.resize(8, 1);
        // "あ" (wide) then "い" (wide) then "x": spacer cells must not be emitted
        g.apply("\x1b[1;1H\x1b[0mあ\x1b[1;3Hい\x1b[1;5Hx");
        // span the whole content: spacer at col 1 and col 3 are dropped
        assert_eq!(g.selection_text((0, 0), (0, 5)), "あいx");
    }

    #[test]
    fn selection_highlight_reverses_selected_cells_only() {
        let mut g = Grid::new();
        g.resize(6, 1);
        g.apply("\x1b[1;1H\x1b[0mabcdef");
        let mut r = Renderer::new();
        r.set_selection(Some(((0, 1), (0, 3)))); // cols 1..=3 → "bcd"
        let out = r.paint(&g, 6, 1);
        // reverse video turned on inside the selection and off after it
        assert!(out.contains("\x1b[7m"));
        assert!(out.contains("\x1b[27m"));
    }
}
