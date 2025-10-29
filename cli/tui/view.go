package tui

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/lipgloss"
)

// View renders the TUI
func (m *Model) View() string {
	if m.width == 0 {
		return "Loading..."
	}

	switch m.mode {
	case ModeHelp:
		return m.renderHelp()
	case ModeTerminal:
		return m.renderTerminalView()
	default:
		return m.renderMain()
	}
}

// renderMain renders the main file browser view
func (m *Model) renderMain() string {
	var sections []string

	// Title bar
	sections = append(sections, m.renderTitle())

	// Main content area (file list + preview)
	sections = append(sections, m.renderContent())

	// Status bar
	sections = append(sections, m.renderStatus())

	// Input area (if in non-terminal input mode)
	if m.mode == ModeInput {
		sections = append(sections, m.renderInput())
	}

	// Help bar
	sections = append(sections, m.renderHelpBar())

	return lipgloss.JoinVertical(lipgloss.Left, sections...)
}

// renderTitle renders the title bar with current path
func (m *Model) renderTitle() string {
	title := fmt.Sprintf("VFS File Manager - %s", m.currentPath)
	return m.theme.TitleStyle.Render(title)
}

// renderContent renders the file list and preview pane
func (m *Model) renderContent() string {
	if m.showPreview {
		// Split view: file list on left, preview on right
		fileList := m.renderFileList()
		preview := m.renderPreview()

		leftWidth := m.width / 2
		rightWidth := m.width - leftWidth - 4 // Account for borders

		fileListBox := m.theme.BorderStyle.
			Width(leftWidth).
			Height(m.getVisibleLines() + 2).
			Render(fileList)

		previewBox := m.theme.PreviewBorderStyle.
			Width(rightWidth).
			Height(m.getVisibleLines() + 2).
			Render(preview)

		return lipgloss.JoinHorizontal(lipgloss.Top, fileListBox, previewBox)
	}

	// Full width file list
	fileList := m.renderFileList()
	return m.theme.BorderStyle.
		Width(m.width - 4).
		Height(m.getVisibleLines() + 2).
		Render(fileList)
}

// renderFileList renders the list of files and directories
func (m *Model) renderFileList() string {
	if len(m.entries) == 0 {
		return m.theme.NormalItemStyle.Render("(empty directory)")
	}

	var lines []string
	visibleLines := m.getVisibleLines()

	start := m.offset
	end := m.offset + visibleLines
	if end > len(m.entries) {
		end = len(m.entries)
	}

	for i := start; i < end; i++ {
		entry := m.entries[i]
		line := m.renderFileEntry(entry, i == m.cursor)
		lines = append(lines, line)
	}

	return strings.Join(lines, "\n")
}

// renderFileEntry renders a single file or directory entry
func (m *Model) renderFileEntry(entry *Entry, selected bool) string {
	// Build entry line: [icon] name size
	icon := entry.Icon()
	name := entry.DisplayName()
	size := entry.DisplaySize()

	// Apply style based on selection and type
	var style lipgloss.Style
	if selected {
		style = m.theme.SelectedItemStyle
	} else if entry.IsDir {
		style = m.theme.DirectoryStyle
	} else {
		style = m.theme.FileStyle
	}

	// Format: "📁 documents/        <DIR>"
	nameWidth := 40
	if m.showPreview {
		nameWidth = 30
	}

	formattedName := name
	if len(name) > nameWidth {
		formattedName = name[:nameWidth-3] + "..."
	} else {
		formattedName = name + strings.Repeat(" ", nameWidth-len(name))
	}

	line := fmt.Sprintf("%s %s %10s", icon, formattedName, size)
	return style.Render(line)
}

// renderPreview renders the file preview pane
func (m *Model) renderPreview() string {
	entry := m.currentEntry()
	if entry == nil {
		return m.theme.PreviewStyle.Render("No file selected")
	}

	if entry.IsDir {
		// Show directory info
		info := fmt.Sprintf("Directory: %s\n\n", entry.Name)
		info += fmt.Sprintf("Path: %s\n", entry.Path)
		info += fmt.Sprintf("Modified: %s\n", entry.DisplayModTime())
		info += fmt.Sprintf("Permissions: %s\n", entry.DisplayMode())
		return m.theme.PreviewStyle.Render(info)
	}

	// Show file preview
	if m.previewError != nil {
		return m.theme.ErrorStyle.Render(fmt.Sprintf("Error: %v", m.previewError))
	}

	if m.previewContent == "" {
		return m.theme.PreviewStyle.Render("(empty file)")
	}

	// Show file info + content preview
	info := fmt.Sprintf("File: %s\n", entry.Name)
	info += fmt.Sprintf("Size: %s\n", entry.DisplaySize())
	info += fmt.Sprintf("Modified: %s\n\n", entry.DisplayModTime())
	info += "--- Preview ---\n"

	// Limit preview lines
	lines := strings.Split(m.previewContent, "\n")
	maxLines := m.getVisibleLines() - 6
	if len(lines) > maxLines {
		lines = lines[:maxLines]
		lines = append(lines, "...")
	}

	info += strings.Join(lines, "\n")

	return m.theme.PreviewStyle.Render(info)
}

// renderStatus renders the status bar
func (m *Model) renderStatus() string {
	// Left side: file count and cursor position
	left := ""
	if len(m.entries) > 0 {
		left = fmt.Sprintf("%d/%d items", m.cursor+1, len(m.entries))
	} else {
		left = "0 items"
	}

	// Right side: status/error messages
	right := ""
	if m.errorMsg != "" {
		right = m.theme.ErrorStyle.Render(m.errorMsg)
	} else if m.statusMsg != "" {
		right = m.statusMsg
	}

	// Calculate spacing
	spacing := max(m.width-lipgloss.Width(left)-lipgloss.Width(right)-4, 0)

	statusLine := left + strings.Repeat(" ", spacing) + right
	return m.theme.StatusBarStyle.Width(m.width).Render(statusLine)
}

// renderInput renders the input field for commands or user input
func (m *Model) renderInput() string {
	prompt := ""
	if m.mode == ModeCommand {
		prompt = ": "
	}

	input := prompt + m.textInput.View()
	return m.theme.CommandStyle.Render(input)
}

// renderTerminalView renders the full-screen terminal mode
func (m *Model) renderTerminalView() string {
	var sections []string

	// Title bar
	title := m.theme.TitleStyle.Render("VFS Terminal - Press # to return to Navigation")
	sections = append(sections, title)

	// Terminal content
	sections = append(sections, m.renderTerminalContent())

	// Help bar
	sections = append(sections, m.renderHelpBar())

	return lipgloss.JoinVertical(lipgloss.Left, sections...)
}

// renderTerminalContent renders the terminal history and input
func (m *Model) renderTerminalContent() string {
	availableHeight := m.height - 6 // Reserve for title, help, padding

	var lines []string

	// Render all terminal history entries
	for _, entry := range m.terminalHistory {
		// Command prompt line: [0] /demo # ls
		promptLine := fmt.Sprintf("[%d] %s > %s",
			entry.Number,
			m.theme.DirectoryStyle.Render(entry.Path),
			entry.Command,
		)
		lines = append(lines, promptLine)

		// Output
		if entry.Output != "" {
			outputLines := strings.Split(strings.TrimSuffix(entry.Output, "\n"), "\n")
			lines = append(lines, outputLines...)
		}

		// Error
		if entry.Error != "" {
			errorLine := m.theme.ErrorStyle.Render("Error: " + entry.Error)
			lines = append(lines, errorLine)
		}

		// Empty line between entries
		lines = append(lines, "")
	}

	// Current input prompt
	currentPrompt := fmt.Sprintf("[%d] %s %s",
		m.commandCounter,
		m.theme.DirectoryStyle.Render(m.currentPath),
		m.textInput.View(),
	)
	lines = append(lines, currentPrompt)

	// Apply scroll offset and limit to available height
	totalLines := len(lines)
	startLine := 0
	if totalLines > availableHeight {
		// Show most recent lines by default (scroll offset of 0)
		startLine = max(totalLines-availableHeight+m.terminalOffset, 0)
		endLine := startLine + availableHeight
		if endLine > totalLines {
			endLine = totalLines
		}
		lines = lines[startLine:endLine]
	}

	content := strings.Join(lines, "\n")

	return m.theme.BorderStyle.
		Width(m.width - 4).
		Height(availableHeight).
		Render(content)
}

// renderHelpBar renders the bottom help bar
func (m *Model) renderHelpBar() string {
	if m.showFullHelp {
		return m.help.View(m.keys)
	}
	return m.theme.HelpStyle.Render(m.help.ShortHelpView(m.keys.ShortHelp()))
}

// renderHelp renders the full help screen
func (m *Model) renderHelp() string {
	var sections []string

	// Title
	title := m.theme.TitleStyle.Render("VFS File Manager - Help")
	sections = append(sections, title)
	sections = append(sections, "")

	// Navigation
	sections = append(sections, m.theme.TitleStyle.Render("Navigation:"))
	sections = append(sections, "  ↑/k        Move up")
	sections = append(sections, "  ↓/j        Move down")
	sections = append(sections, "  PgUp/Ctrl+U  Page up")
	sections = append(sections, "  PgDn/Ctrl+D  Page down")
	sections = append(sections, "  Home/g     Go to top")
	sections = append(sections, "  End/G      Go to bottom")
	sections = append(sections, "  Enter/l    Enter directory / Open file")
	sections = append(sections, "  Backspace/h  Go to parent directory")
	sections = append(sections, "")

	// File Operations
	sections = append(sections, m.theme.TitleStyle.Render("File Operations:"))
	sections = append(sections, "  n          Create new file")
	sections = append(sections, "  N          Create new directory")
	sections = append(sections, "  d/Del      Delete selected item")
	sections = append(sections, "  r          Rename selected item")
	sections = append(sections, "  y          Copy path to clipboard")
	sections = append(sections, "")

	// View
	sections = append(sections, m.theme.TitleStyle.Render("View:"))
	sections = append(sections, "  p          Toggle preview pane")
	sections = append(sections, "  Ctrl+R     Refresh current directory")
	sections = append(sections, "")

	// Terminal
	sections = append(sections, m.theme.TitleStyle.Render("Terminal:"))
	sections = append(sections, "  #          Toggle terminal window")
	sections = append(sections, "             (Execute VFS commands)")
	sections = append(sections, "  Esc        Exit terminal input (terminal stays open)")
	sections = append(sections, "")

	// Application
	sections = append(sections, m.theme.TitleStyle.Render("Application:"))
	sections = append(sections, "  ?          Toggle this help")
	sections = append(sections, "  q/Ctrl+C   Quit")
	sections = append(sections, "")

	sections = append(sections, m.theme.HelpStyle.Render("Press ? or q to return"))

	return lipgloss.JoinVertical(lipgloss.Left, sections...)
}
