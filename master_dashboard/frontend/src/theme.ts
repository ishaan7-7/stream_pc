import { createTheme } from '@mui/material/styles';

export const industrialTheme = createTheme({
  palette: {
    mode: 'light',
    background: {
      default: '#f4f6f8',
      paper: '#ffffff',
    },
    primary: { main: '#2c3e50' },
    error: { main: '#d32f2f' },
    warning: { main: '#ed6c02' },
    success: { main: '#2e7d32' },
  },
  typography: {
    fontFamily: '"Roboto", "Helvetica", "Arial", sans-serif',
    h6: {
      fontWeight: 600,
      letterSpacing: '0.5px',
      textTransform: 'uppercase',
      fontSize: '0.9rem',
    },
  },
  shape: { borderRadius: 0 },
  components: {
    MuiButton: {
      styleOverrides: {
        root: { textTransform: 'none', fontWeight: 600 },
      },
    },
    MuiPaper: {
      styleOverrides: {
        root: {
          border: '1px solid #e0e0e0',
          boxShadow: 'none',
        },
      },
    },
  },
});