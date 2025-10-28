package tui

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/charmbracelet/bubbles/help"
	"github.com/charmbracelet/bubbles/key"
	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/mwantia/vfs/command"
)

// Mode represents the current interaction mode
type Mode int

const (
	ModeNormal Mode = iota
	ModeCommand
	ModeInput
	ModeHelp
)

// InputType represents what kind of input we're collecting
type InputType int

const (
	InputNewFile InputType = iota
	InputNewDir
	InputRename
	InputDelete
	InputCommand
)

// Model represents the state of the TUI application
type Model struct {
	// Core components
	adapter *VFSAdapter
	cmd     *command.CommandCenter
	theme   *Theme
	keys    KeyMap
	help    help.Model

	// Navigation state
	currentPath string
	previousDir string // Name of directory we came from (for breadcrumb navigation)
	entries     []*Entry
	cursor      int
	offset      int

	// View state
	width          int
	height         int
	showPreview    bool
	previewContent string
	previewError   error
	previewGen     int // Generation counter to prevent race conditions

	// Mode state
	mode      Mode
	inputType InputType
	textInput textinput.Model

	// Status
	statusMsg  string
	errorMsg   string
	commandOut string

	// Clipboard
	clipboard string

	// Help
	showFullHelp bool
}

// NewModel creates a new TUI model
func NewModel(adapter *VFSAdapter, cmd *command.CommandCenter) *Model {
	ti := textinput.New()
	ti.Placeholder = "Enter command..."
	ti.CharLimit = 256

	return &Model{
		adapter:      adapter,
		cmd:          cmd,
		theme:        DefaultTheme(),
		keys:         DefaultKeyMap(),
		help:         help.New(),
		currentPath:  "/",
		showPreview:  true,
		textInput:    ti,
		showFullHelp: false,
	}
}

// Init initializes the model
func (m *Model) Init() tea.Cmd {
	return tea.Batch(
		m.loadDirectory(),
		textinput.Blink,
	)
}

// Update handles messages and updates the model
func (m *Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		m.help.Width = msg.Width
		return m, nil

	case directoryLoadedMsg:
		m.entries = msg.entries
		m.errorMsg = ""

		// Position cursor on previous directory if we just navigated back
		if m.previousDir != "" {
			DebugLog("Positioning cursor on previous directory: '%s'", m.previousDir)
			for i, entry := range m.entries {
				if entry.Name == m.previousDir {
					m.cursor = i
					// Adjust offset to keep cursor visible
					visibleLines := m.getVisibleLines()
					if m.cursor >= m.offset+visibleLines {
						m.offset = m.cursor - visibleLines + 1
					} else if m.cursor < m.offset {
						m.offset = m.cursor
					}
					DebugLog("  Found at index %d, offset=%d", i, m.offset)
					break
				}
			}
			m.previousDir = "" // Clear after using
		} else {
			// Normal navigation - position at top
			if len(m.entries) > 0 && m.cursor >= len(m.entries) {
				m.cursor = len(m.entries) - 1
			}
			if m.cursor < 0 {
				m.cursor = 0
			}
		}
		return m, m.updatePreview()

	case previewLoadedMsg:
		// Only update if this preview is for the current generation
		if msg.generation == m.previewGen {
			m.previewContent = msg.content
			m.previewError = msg.err
		} else {
			DebugLog("Ignoring stale preview (gen %d, current %d)", msg.generation, m.previewGen)
		}
		return m, nil

	case commandExecutedMsg:
		m.commandOut = msg.output
		m.errorMsg = msg.error
		m.statusMsg = "Command executed"
		return m, m.loadDirectory()

	case errorMsg:
		m.errorMsg = string(msg)
		return m, nil

	case tea.KeyMsg:
		return m.handleKeyPress(msg)
	}

	// Handle text input updates when in input mode
	if m.mode == ModeCommand || m.mode == ModeInput {
		var cmd tea.Cmd
		m.textInput, cmd = m.textInput.Update(msg)
		return m, cmd
	}

	return m, nil
}

// handleKeyPress processes keyboard input based on current mode
func (m *Model) handleKeyPress(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	// Handle mode-specific inputs
	switch m.mode {
	case ModeCommand, ModeInput:
		return m.handleInputMode(msg)
	case ModeHelp:
		return m.handleHelpMode(msg)
	case ModeNormal:
		return m.handleNormalMode(msg)
	}

	return m, nil
}

// handleNormalMode processes keys in normal browsing mode
func (m *Model) handleNormalMode(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch {
	case key.Matches(msg, m.keys.Quit):
		return m, tea.Quit

	case key.Matches(msg, m.keys.Help):
		m.mode = ModeHelp
		m.showFullHelp = !m.showFullHelp
		return m, nil

	case key.Matches(msg, m.keys.Up):
		m.moveCursor(-1)
		return m, m.updatePreview()

	case key.Matches(msg, m.keys.Down):
		m.moveCursor(1)
		return m, m.updatePreview()

	case key.Matches(msg, m.keys.PageUp):
		m.moveCursor(-10)
		return m, m.updatePreview()

	case key.Matches(msg, m.keys.PageDown):
		m.moveCursor(10)
		return m, m.updatePreview()

	case key.Matches(msg, m.keys.Top):
		m.cursor = 0
		m.offset = 0
		return m, m.updatePreview()

	case key.Matches(msg, m.keys.Bottom):
		if len(m.entries) > 0 {
			m.cursor = len(m.entries) - 1
		}
		return m, m.updatePreview()

	case key.Matches(msg, m.keys.Enter):
		return m, m.enterDirectory()

	case key.Matches(msg, m.keys.Back):
		return m, m.goBack()

	case key.Matches(msg, m.keys.TogglePreview):
		m.showPreview = !m.showPreview
		return m, nil

	case key.Matches(msg, m.keys.Refresh):
		return m, m.loadDirectory()

	case key.Matches(msg, m.keys.NewFile):
		m.startInput(InputNewFile, "New file name:")
		return m, nil

	case key.Matches(msg, m.keys.NewDir):
		m.startInput(InputNewDir, "New directory name:")
		return m, nil

	case key.Matches(msg, m.keys.Delete):
		if m.currentEntry() != nil {
			m.startInput(InputDelete, fmt.Sprintf("Delete %s? (y/n):", m.currentEntry().Name))
		}
		return m, nil

	case key.Matches(msg, m.keys.Rename):
		if m.currentEntry() != nil {
			m.startInput(InputRename, "New name:")
			m.textInput.SetValue(m.currentEntry().Name)
		}
		return m, nil

	case key.Matches(msg, m.keys.Copy):
		if entry := m.currentEntry(); entry != nil {
			m.clipboard = entry.Path
			m.statusMsg = fmt.Sprintf("Copied: %s", entry.Name)
		}
		return m, nil

	case key.Matches(msg, m.keys.Command):
		m.startInput(InputCommand, ":")
		return m, nil
	}

	return m, nil
}

// handleInputMode processes keys when collecting user input
func (m *Model) handleInputMode(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.Type {
	case tea.KeyEscape:
		m.cancelInput()
		return m, nil

	case tea.KeyEnter:
		return m, m.submitInput()
	}

	var cmd tea.Cmd
	m.textInput, cmd = m.textInput.Update(msg)
	return m, cmd
}

// handleHelpMode processes keys in help mode
func (m *Model) handleHelpMode(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch {
	case key.Matches(msg, m.keys.Help), key.Matches(msg, m.keys.Quit):
		m.mode = ModeNormal
		return m, nil
	}
	return m, nil
}

// startInput enters input mode with the specified type and prompt
func (m *Model) startInput(inputType InputType, prompt string) {
	m.mode = ModeInput
	m.inputType = inputType
	m.textInput.Placeholder = prompt
	m.textInput.SetValue("")
	m.textInput.Focus()
	m.errorMsg = ""
	m.statusMsg = ""

	if inputType == InputCommand {
		m.mode = ModeCommand
	}
}

// cancelInput exits input mode without taking action
func (m *Model) cancelInput() {
	m.mode = ModeNormal
	m.textInput.Blur()
	m.textInput.SetValue("")
}

// submitInput processes the collected input
func (m *Model) submitInput() tea.Cmd {
	value := strings.TrimSpace(m.textInput.Value())
	m.cancelInput()

	if value == "" {
		return nil
	}

	switch m.inputType {
	case InputNewFile:
		return m.createFile(value)
	case InputNewDir:
		return m.createDirectory(value)
	case InputRename:
		return m.renameEntry(value)
	case InputDelete:
		if strings.ToLower(value) == "y" || strings.ToLower(value) == "yes" {
			return m.deleteEntry()
		}
		return nil
	case InputCommand:
		return m.executeCommand(value)
	}

	return nil
}

// moveCursor moves the cursor by delta, handling bounds and scrolling
func (m *Model) moveCursor(delta int) {
	if len(m.entries) == 0 {
		return
	}

	m.cursor += delta

	// Clamp cursor
	if m.cursor < 0 {
		m.cursor = 0
	}
	if m.cursor >= len(m.entries) {
		m.cursor = len(m.entries) - 1
	}

	// Adjust offset for scrolling
	visibleLines := m.getVisibleLines()
	if m.cursor < m.offset {
		m.offset = m.cursor
	}
	if m.cursor >= m.offset+visibleLines {
		m.offset = m.cursor - visibleLines + 1
	}
}

// getVisibleLines returns how many file entries can be displayed
func (m *Model) getVisibleLines() int {
	// Reserve space for title, status bar, help, and padding
	reserved := 8
	available := m.height - reserved
	if available < 5 {
		return 5
	}
	return available
}

// currentEntry returns the currently selected entry
func (m *Model) currentEntry() *Entry {
	if m.cursor >= 0 && m.cursor < len(m.entries) {
		return m.entries[m.cursor]
	}
	return nil
}

// Messages for async operations
type directoryLoadedMsg struct {
	entries []*Entry
}

type previewLoadedMsg struct {
	content    string
	err        error
	generation int // Which preview request this is for
}

type commandExecutedMsg struct {
	output string
	error  string
}

type errorMsg string

// Commands for async operations
func (m *Model) loadDirectory() tea.Cmd {
	return func() tea.Msg {
		entries, err := m.adapter.ListDirectory(m.currentPath)
		if err != nil {
			return errorMsg(fmt.Sprintf("Failed to load directory: %v", err))
		}
		return directoryLoadedMsg{entries: entries}
	}
}

func (m *Model) updatePreview() tea.Cmd {
	if !m.showPreview {
		return nil
	}

	entry := m.currentEntry()

	// Increment generation counter for new preview
	m.previewGen++
	currentGen := m.previewGen

	if entry == nil || entry.IsDir {
		return func() tea.Msg {
			return previewLoadedMsg{content: "", err: nil, generation: currentGen}
		}
	}

	// Capture entry path to prevent race
	entryPath := entry.Path
	entryName := entry.Name

	return func() tea.Msg {
		DebugLog("Loading preview gen=%d for: %s", currentGen, entryName)

		// Calculate available space for preview
		previewWidth := m.width / 2
		previewHeight := m.height - 10

		// Use new preview system that handles different file types
		content, err := m.adapter.GeneratePreview(entryPath, previewWidth, previewHeight)

		if err != nil {
			DebugLog("Preview gen=%d failed for %s: %v", currentGen, entryName, err)
		} else {
			DebugLog("Preview gen=%d loaded for %s (%d bytes)", currentGen, entryName, len(content))
		}

		return previewLoadedMsg{content: content, err: err, generation: currentGen}
	}
}

func (m *Model) enterDirectory() tea.Cmd {
	entry := m.currentEntry()
	if entry == nil {
		DebugLog("enterDirectory: No entry selected")
		return nil
	}

	DebugLog("enterDirectory: Selected '%s' (IsDir=%v, Mode=%s)", entry.Name, entry.IsDir, entry.Mode.String())

	if !entry.IsDir {
		m.statusMsg = fmt.Sprintf("Cannot open file: %s", entry.Name)
		DebugLog("  Not a directory, cannot enter")
		return nil
	}

	DebugLog("  Navigating to: %s", entry.Path)
	m.currentPath = entry.Path
	m.previousDir = "" // Clear previous directory when entering new one
	m.cursor = 0
	m.offset = 0
	return m.loadDirectory()
}

func (m *Model) goBack() tea.Cmd {
	if m.currentPath == "/" {
		return nil
	}

	// Remember which directory we're leaving so we can position cursor on it
	m.previousDir = filepath.Base(m.currentPath)
	DebugLog("goBack: Leaving '%s', will position cursor on it", m.previousDir)

	m.currentPath = filepath.Dir(m.currentPath)
	m.cursor = 0
	m.offset = 0
	return m.loadDirectory()
}

func (m *Model) createFile(name string) tea.Cmd {
	return func() tea.Msg {
		path := filepath.Join(m.currentPath, name)
		if err := m.adapter.CreateFile(path); err != nil {
			return errorMsg(fmt.Sprintf("Failed to create file: %v", err))
		}
		return m.loadDirectory()()
	}
}

func (m *Model) createDirectory(name string) tea.Cmd {
	return func() tea.Msg {
		path := filepath.Join(m.currentPath, name)
		if err := m.adapter.CreateDirectory(path); err != nil {
			return errorMsg(fmt.Sprintf("Failed to create directory: %v", err))
		}
		return m.loadDirectory()()
	}
}

func (m *Model) deleteEntry() tea.Cmd {
	entry := m.currentEntry()
	if entry == nil {
		return nil
	}

	return func() tea.Msg {
		var err error
		if entry.IsDir {
			err = m.adapter.DeleteRecursive(entry.Path)
		} else {
			err = m.adapter.Delete(entry.Path, false)
		}

		if err != nil {
			return errorMsg(fmt.Sprintf("Failed to delete: %v", err))
		}
		return m.loadDirectory()()
	}
}

func (m *Model) renameEntry(newName string) tea.Cmd {
	entry := m.currentEntry()
	if entry == nil {
		return nil
	}

	return func() tea.Msg {
		// For now, we'll implement rename as copy + delete
		// since VFS doesn't have Rename implemented yet
		newPath := filepath.Join(m.currentPath, newName)

		if !entry.IsDir {
			if err := m.adapter.CopyFile(entry.Path, newPath); err != nil {
				return errorMsg(fmt.Sprintf("Failed to rename: %v", err))
			}
			if err := m.adapter.Delete(entry.Path, false); err != nil {
				return errorMsg(fmt.Sprintf("Failed to remove old file: %v", err))
			}
		} else {
			return errorMsg("Directory rename not yet supported")
		}

		return m.loadDirectory()()
	}
}

func (m *Model) executeCommand(cmdLine string) tea.Cmd {
	return func() tea.Msg {
		// Parse command line
		args := parseCommandLine(cmdLine)
		if len(args) == 0 {
			return commandExecutedMsg{output: "", error: ""}
		}

		// Execute command through command center
		exitCode, err := m.cmd.Execute(m.adapter.ctx, m.adapter.vfs, args...)

		output := ""
		errStr := ""

		if err != nil {
			errStr = err.Error()
		}

		if exitCode != 0 {
			errStr = fmt.Sprintf("Command exited with code %d", exitCode)
		}

		return commandExecutedMsg{output: output, error: errStr}
	}
}

// parseCommandLine splits a command line into tokens (same as original main.go)
func parseCommandLine(line string) []string {
	var args []string
	var current strings.Builder
	inQuote := false
	quoteChar := rune(0)

	for _, ch := range line {
		switch {
		case ch == '"' || ch == '\'':
			if inQuote {
				if ch == quoteChar {
					inQuote = false
					quoteChar = 0
				} else {
					current.WriteRune(ch)
				}
			} else {
				inQuote = true
				quoteChar = ch
			}

		case ch == ' ' && !inQuote:
			if current.Len() > 0 {
				args = append(args, current.String())
				current.Reset()
			}

		default:
			current.WriteRune(ch)
		}
	}

	if current.Len() > 0 {
		args = append(args, current.String())
	}

	return args
}
