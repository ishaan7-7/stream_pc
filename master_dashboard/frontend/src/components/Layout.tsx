import { Box, Drawer, List, ListItem, ListItemButton, ListItemIcon, ListItemText, Typography, AppBar, Toolbar, Button } from '@mui/material';
import { Outlet, useNavigate, useLocation } from 'react-router-dom';
import SpeedIcon from '@mui/icons-material/Speed';
import PsychologyIcon from '@mui/icons-material/Psychology';
import HealthAndSafetyIcon from '@mui/icons-material/HealthAndSafety';
import WarningAmberIcon from '@mui/icons-material/WarningAmber';
import PlayCircleOutlineIcon from '@mui/icons-material/PlayCircleOutline';
import { useStore } from '../store';

const DRAWER_WIDTH = 240;

const menuItems = [
  { text: 'System Ops', path: '/', icon: <SpeedIcon />, index: 0 },
  { text: 'Inference Engine', path: '/inference', icon: <PsychologyIcon />, index: 1 },
  { text: 'Vehicle Health', path: '/gold', icon: <HealthAndSafetyIcon />, index: 2 },
  { text: 'Alerts Management', path: '/alerts', icon: <WarningAmberIcon />, index: 3 },
  { text: 'Telemetry Replay', path: '/replay', icon: <PlayCircleOutlineIcon />, index: 4 },
];

export default function Layout() {
  const navigate = useNavigate();
  const location = useLocation();
  const { autoRefresh, toggleAutoRefresh, setActiveTab } = useStore();

  const handleNavigation = (path: string, index: number) => {
    setActiveTab(index);
    navigate(path);
  };

  return (
    <Box sx={{ display: 'flex', height: '100vh', overflow: 'hidden' }}>
      {/* Top Header Bar */}
      <AppBar position="fixed" sx={{ zIndex: (theme) => theme.zIndex.drawer + 1, bgcolor: 'primary.main', boxShadow: 'none', borderBottom: '1px solid #1a252f' }}>
        <Toolbar sx={{ justifyContent: 'space-between', minHeight: '48px !important' }}>
          <Typography variant="h6" color="inherit">
            Streaming Emulator // Master Control
          </Typography>
          <Button 
            variant="contained" 
            color={autoRefresh ? "success" : "error"}
            onClick={toggleAutoRefresh}
            size="small"
            disableElevation
            sx={{ borderRadius: 0 }}
          >
            {autoRefresh ? "LIVE REFRESH: ON" : "LIVE REFRESH: OFF"}
          </Button>
        </Toolbar>
      </AppBar>

      {/* Permanent Sidebar */}
      <Drawer
        variant="permanent"
        sx={{
          width: DRAWER_WIDTH,
          flexShrink: 0,
          [`& .MuiDrawer-paper`]: { width: DRAWER_WIDTH, boxSizing: 'border-box', borderRight: '1px solid #e0e0e0', bgcolor: 'background.paper' },
        }}
      >
        <Toolbar sx={{ minHeight: '48px !important' }} />
        <Box sx={{ overflow: 'auto', mt: 2 }}>
          <List>
            {menuItems.map((item) => (
              <ListItem key={item.text} disablePadding>
                <ListItemButton 
                  selected={location.pathname === item.path}
                  onClick={() => handleNavigation(item.path, item.index)}
                  sx={{
                    '&.Mui-selected': { bgcolor: 'rgba(44, 62, 80, 0.08)', borderRight: '4px solid #2c3e50' },
                    '&:hover': { bgcolor: 'rgba(44, 62, 80, 0.04)' }
                  }}
                >
                  <ListItemIcon sx={{ color: location.pathname === item.path ? 'primary.main' : 'text.secondary', minWidth: 40 }}>
                    {item.icon}
                  </ListItemIcon>
                  <ListItemText 
                    primary={item.text} 
                    primaryTypographyProps={{ fontWeight: location.pathname === item.path ? 700 : 500, fontSize: '0.85rem' }}
                  />
                </ListItemButton>
              </ListItem>
            ))}
          </List>
        </Box>
      </Drawer>

      {/* Main Content Viewport */}
      <Box component="main" sx={{ flexGrow: 1, p: 3, bgcolor: 'background.default', mt: '48px', overflow: 'auto' }}>
        <Outlet />
      </Box>
    </Box>
  );
}