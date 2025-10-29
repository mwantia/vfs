package tui

import "github.com/charmbracelet/bubbles/key"

// KeyMap defines keyboard shortcuts for the file manager
type KeyMap struct {
	// Navigation
	Up        key.Binding
	Down      key.Binding
	PageUp    key.Binding
	PageDown  key.Binding
	Top       key.Binding
	Bottom    key.Binding
	Enter     key.Binding
	Back      key.Binding

	// File operations
	Delete    key.Binding
	Rename    key.Binding
	Copy      key.Binding
	NewFile   key.Binding
	NewDir    key.Binding

	// View
	TogglePreview key.Binding
	Refresh       key.Binding

	// Command mode
	Command key.Binding

	// Application
	Quit key.Binding
	Help key.Binding
}

// DefaultKeyMap returns the default key bindings
func DefaultKeyMap() KeyMap {
	return KeyMap{
		// Navigation
		Up: key.NewBinding(
			key.WithKeys("up", "k"),
			key.WithHelp("↑/k", "up"),
		),
		Down: key.NewBinding(
			key.WithKeys("down", "j"),
			key.WithHelp("↓/j", "down"),
		),
		PageUp: key.NewBinding(
			key.WithKeys("pgup", "ctrl+u"),
			key.WithHelp("pgup/ctrl+u", "page up"),
		),
		PageDown: key.NewBinding(
			key.WithKeys("pgdown", "ctrl+d"),
			key.WithHelp("pgdn/ctrl+d", "page down"),
		),
		Top: key.NewBinding(
			key.WithKeys("home", "g"),
			key.WithHelp("home/g", "top"),
		),
		Bottom: key.NewBinding(
			key.WithKeys("end", "G"),
			key.WithHelp("end/G", "bottom"),
		),
		Enter: key.NewBinding(
			key.WithKeys("enter", "l"),
			key.WithHelp("enter/l", "open"),
		),
		Back: key.NewBinding(
			key.WithKeys("backspace", "h"),
			key.WithHelp("bksp/h", "back"),
		),

		// File operations
		Delete: key.NewBinding(
			key.WithKeys("d", "delete"),
			key.WithHelp("d/del", "delete"),
		),
		Rename: key.NewBinding(
			key.WithKeys("r"),
			key.WithHelp("r", "rename"),
		),
		Copy: key.NewBinding(
			key.WithKeys("y"),
			key.WithHelp("y", "copy"),
		),
		NewFile: key.NewBinding(
			key.WithKeys("n"),
			key.WithHelp("n", "new file"),
		),
		NewDir: key.NewBinding(
			key.WithKeys("N"),
			key.WithHelp("N", "new dir"),
		),

		// View
		TogglePreview: key.NewBinding(
			key.WithKeys("p"),
			key.WithHelp("p", "toggle preview"),
		),
		Refresh: key.NewBinding(
			key.WithKeys("ctrl+r"),
			key.WithHelp("ctrl+r", "refresh"),
		),

		// Command mode
		Command: key.NewBinding(
			key.WithKeys("#"),
			key.WithHelp("#", "toggle terminal"),
		),

		// Application
		Quit: key.NewBinding(
			key.WithKeys("q", "ctrl+c"),
			key.WithHelp("q/ctrl+c", "quit"),
		),
		Help: key.NewBinding(
			key.WithKeys("?"),
			key.WithHelp("?", "help"),
		),
	}
}

// ShortHelp returns a brief help text
func (k KeyMap) ShortHelp() []key.Binding {
	return []key.Binding{k.Up, k.Down, k.Enter, k.Back, k.Command, k.Quit, k.Help}
}

// FullHelp returns detailed help text
func (k KeyMap) FullHelp() [][]key.Binding {
	return [][]key.Binding{
		{k.Up, k.Down, k.PageUp, k.PageDown, k.Top, k.Bottom},
		{k.Enter, k.Back, k.TogglePreview, k.Refresh},
		{k.NewFile, k.NewDir, k.Copy, k.Rename, k.Delete},
		{k.Command, k.Help, k.Quit},
	}
}
