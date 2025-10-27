package tui

import (
	"github.com/charmbracelet/lipgloss"
)

// Theme defines the color scheme and styles for the TUI
type Theme struct {
	// Base colors
	Primary       lipgloss.Color
	Secondary     lipgloss.Color
	Background    lipgloss.Color
	Foreground    lipgloss.Color
	Border        lipgloss.Color
	Highlight     lipgloss.Color
	HighlightText lipgloss.Color
	Dim           lipgloss.Color
	Error         lipgloss.Color
	Success       lipgloss.Color
	Warning       lipgloss.Color

	// Styles
	TitleStyle         lipgloss.Style
	StatusBarStyle     lipgloss.Style
	SelectedItemStyle  lipgloss.Style
	NormalItemStyle    lipgloss.Style
	DirectoryStyle     lipgloss.Style
	FileStyle          lipgloss.Style
	BorderStyle        lipgloss.Style
	PreviewStyle       lipgloss.Style
	PreviewBorderStyle lipgloss.Style
	ErrorStyle         lipgloss.Style
	HelpStyle          lipgloss.Style
	CommandStyle       lipgloss.Style
}

// DefaultTheme returns a default dark theme
func DefaultTheme() *Theme {
	t := &Theme{
		Primary:       lipgloss.Color("#7AA2F7"),
		Secondary:     lipgloss.Color("#BB9AF7"),
		Background:    lipgloss.Color("#1A1B26"),
		Foreground:    lipgloss.Color("#C0CAF5"),
		Border:        lipgloss.Color("#414868"),
		Highlight:     lipgloss.Color("#7AA2F7"),
		HighlightText: lipgloss.Color("#1A1B26"),
		Dim:           lipgloss.Color("#565F89"),
		Error:         lipgloss.Color("#F7768E"),
		Success:       lipgloss.Color("#9ECE6A"),
		Warning:       lipgloss.Color("#E0AF68"),
	}

	t.TitleStyle = lipgloss.NewStyle().
		Foreground(t.Primary).
		Bold(true).
		Padding(0, 1)

	t.StatusBarStyle = lipgloss.NewStyle().
		Foreground(t.Foreground).
		Background(t.Border).
		Padding(0, 1)

	t.SelectedItemStyle = lipgloss.NewStyle().
		Foreground(t.HighlightText).
		Background(t.Highlight).
		Bold(true)

	t.NormalItemStyle = lipgloss.NewStyle().
		Foreground(t.Foreground)

	t.DirectoryStyle = lipgloss.NewStyle().
		Foreground(t.Primary).
		Bold(true)

	t.FileStyle = lipgloss.NewStyle().
		Foreground(t.Foreground)

	t.BorderStyle = lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(t.Border)

	t.PreviewStyle = lipgloss.NewStyle().
		Foreground(t.Foreground).
		Padding(1)

	t.PreviewBorderStyle = lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(t.Border).
		Padding(0, 1)

	t.ErrorStyle = lipgloss.NewStyle().
		Foreground(t.Error).
		Bold(true)

	t.HelpStyle = lipgloss.NewStyle().
		Foreground(t.Dim).
		Padding(0, 1)

	t.CommandStyle = lipgloss.NewStyle().
		Foreground(t.Success).
		Bold(true)

	return t
}

// GruvboxTheme returns a gruvbox-inspired theme
func GruvboxTheme() *Theme {
	t := &Theme{
		Primary:       lipgloss.Color("#83A598"),
		Secondary:     lipgloss.Color("#D3869B"),
		Background:    lipgloss.Color("#282828"),
		Foreground:    lipgloss.Color("#EBDBB2"),
		Border:        lipgloss.Color("#504945"),
		Highlight:     lipgloss.Color("#83A598"),
		HighlightText: lipgloss.Color("#282828"),
		Dim:           lipgloss.Color("#928374"),
		Error:         lipgloss.Color("#FB4934"),
		Success:       lipgloss.Color("#B8BB26"),
		Warning:       lipgloss.Color("#FABD2F"),
	}

	t.TitleStyle = lipgloss.NewStyle().
		Foreground(t.Primary).
		Bold(true).
		Padding(0, 1)

	t.StatusBarStyle = lipgloss.NewStyle().
		Foreground(t.Foreground).
		Background(t.Border).
		Padding(0, 1)

	t.SelectedItemStyle = lipgloss.NewStyle().
		Foreground(t.HighlightText).
		Background(t.Highlight).
		Bold(true)

	t.NormalItemStyle = lipgloss.NewStyle().
		Foreground(t.Foreground)

	t.DirectoryStyle = lipgloss.NewStyle().
		Foreground(t.Primary).
		Bold(true)

	t.FileStyle = lipgloss.NewStyle().
		Foreground(t.Foreground)

	t.BorderStyle = lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(t.Border)

	t.PreviewStyle = lipgloss.NewStyle().
		Foreground(t.Foreground).
		Padding(1)

	t.PreviewBorderStyle = lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(t.Border).
		Padding(0, 1)

	t.ErrorStyle = lipgloss.NewStyle().
		Foreground(t.Error).
		Bold(true)

	t.HelpStyle = lipgloss.NewStyle().
		Foreground(t.Dim).
		Padding(0, 1)

	t.CommandStyle = lipgloss.NewStyle().
		Foreground(t.Success).
		Bold(true)

	return t
}
