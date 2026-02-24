import { Box, Typography, Button, Paper } from '@mui/material';
import { useStore } from './store';

function App() {
  const { activeTab, autoRefresh, toggleAutoRefresh } = useStore();

  return (
    <Box sx={{ p: 4, height: '100vh', display: 'flex', flexDirection: 'column', gap: 2 }}>
      <Paper sx={{ p: 2, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <Typography variant="h6" color="primary">
          Streaming Emulator // Master Dashboard
        </Typography>
        <Button 
          variant="contained" 
          color={autoRefresh ? "success" : "error"}
          onClick={toggleAutoRefresh}
        >
          {autoRefresh ? "Auto-Refresh: ON" : "Auto-Refresh: OFF"}
        </Button>
      </Paper>

      <Paper sx={{ p: 4, flexGrow: 1, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
        <Typography variant="body1" color="textSecondary">
          Foundation initialized. Active Tab Index: {activeTab}
        </Typography>
      </Paper>
    </Box>
  );
}

export default App;