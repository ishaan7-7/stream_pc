import React, { useState } from 'react';
import { 
  Box, Typography, Paper, ToggleButtonGroup, ToggleButton, Chip, Divider, 
  Select, MenuItem, FormControl, InputLabel 
} from '@mui/material';
import { AgGridReact } from 'ag-grid-react';
import type { ColDef } from 'ag-grid-community';
import { useQuery } from '@tanstack/react-query';
import axios from 'axios';
import { useStore } from '../store';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, LineChart, Line } from 'recharts';

// --- API Fetcher ---
const fetchObserverSnapshot = async () => {
  const { data } = await axios.get('http://127.0.0.1:8005/api/observer/snapshot');
  return data;
};

export default function DashboardObserver() {
  const { autoRefresh } = useStore();
  const [activeTab, setActiveTab] = useState<'leaderboard' | 'analytics' | 'inspector'>('leaderboard');
  const [inspectorVid, setInspectorVid] = useState<string>('');
  const [inspectorSource, setInspectorSource] = useState<string>('ALL (Latest)');

  const { data, isLoading, isError } = useQuery({
    queryKey: ['observerSnapshot'],
    queryFn: fetchObserverSnapshot,
    refetchInterval: autoRefresh ? 1500 : false,
  });

  const vehicles = data?.vehicles || [];
  const systemHealth = data?.system_health || {};
  const globalStats = data?.global_stats || { total_rows: 0, active_vehicles: 0, avg_latency: 0, dlq_backlog: 0 };

  // Set default inspector vehicle if empty and data arrives
  if (vehicles.length > 0 && !inspectorVid) {
    setInspectorVid(vehicles[0].vehicle_id);
  }

  const selectedInspectorV = vehicles.find((v: any) => v.vehicle_id === inspectorVid) || vehicles[0];
  const availableInspectorSources = ["ALL (Latest)", ...(selectedInspectorV?.module_payloads ? Object.keys(selectedInspectorV.module_payloads) : [])];

  const columnDefs: ColDef[] = [
    { field: 'vehicle_id', headerName: 'VEHICLE ID', flex: 1, minWidth: 150, cellStyle: { fontWeight: 'bold', color: '#1976d2' } },
    { field: 'rows_processed', headerName: 'PROCESSED', flex: 1, type: 'numericColumn', valueFormatter: p => p.value?.toLocaleString() },
    { field: 'rejected_rows', headerName: 'REJECTED', flex: 1, type: 'numericColumn', cellStyle: { color: '#d32f2f' } },
    { 
      field: 'validation_rate', 
      headerName: 'QUALITY SCORE', 
      flex: 1, 
      valueFormatter: p => `${p.value?.toFixed(1)}%`,
      cellStyle: (p) => ({ color: p.value > 95 ? '#2e7d32' : '#d32f2f', fontWeight: 'bold' })
    },
    { field: 'avg_latency', headerName: 'LATENCY (ms)', flex: 1, type: 'numericColumn', valueFormatter: p => p.value?.toFixed(1) },
    { field: 'last_seen_sec', headerName: 'LAST SEEN (s)', flex: 1, type: 'numericColumn', valueFormatter: p => `${p.value?.toFixed(1)}s ago` },
  ];

  if (isLoading && !data) return <Box p={3}><Typography>INITIALIZING TELEMETRY STREAM...</Typography></Box>;

  return (
    <Box sx={{ height: 'calc(100vh - 80px)', display: 'flex', flexDirection: 'column', gap: 2, p: 2, bgcolor: '#f5f5f5' }}>
      
      {/* HEADER & TABS */}
      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-end', borderBottom: '2px solid #bdbdbd', pb: 1 }}>
        <Typography variant="h5" sx={{ fontWeight: 700, color: '#212121', letterSpacing: '-0.5px' }}>
          TELEMETRY OBSERVER
        </Typography>
        <ToggleButtonGroup value={activeTab} exclusive onChange={(e, val) => val && setActiveTab(val)} size="small" sx={{ bgcolor: 'white' }}>
          <ToggleButton value="leaderboard" sx={{ fontWeight: 'bold', px: 3, borderRadius: 0 }}>LEADERBOARD</ToggleButton>
          <ToggleButton value="analytics" sx={{ fontWeight: 'bold', px: 3, borderRadius: 0 }}>FLEET ANALYTICS</ToggleButton>
          <ToggleButton value="inspector" sx={{ fontWeight: 'bold', px: 3, borderRadius: 0 }}>LIVE INSPECTOR</ToggleButton>
        </ToggleButtonGroup>
      </Box>

      {/* HEALTH & KPIs */}
      <Box sx={{ display: 'flex', gap: 2, width: '100%' }}>
        <Paper sx={{ p: 2, borderRadius: 0, display: 'flex', gap: 2, flexWrap: 'wrap', flex: 1.5, borderLeft: '4px solid #1976d2' }}>
          {Object.entries(systemHealth).map(([name, isUp]) => (
            <Chip key={name} label={name} color={isUp ? "success" : "error"} size="small" variant={isUp ? "outlined" : "filled"} sx={{ borderRadius: '2px', fontWeight: 'bold' }} />
          ))}
        </Paper>
        <Paper sx={{ flex: 1, p: 2, borderRadius: 0, borderLeft: '4px solid #424242' }}>
          <Typography variant="caption" sx={{ color: '#757575', fontWeight: 'bold' }}>THROUGHPUT</Typography>
          <Typography variant="h5" sx={{ fontWeight: 'bold' }}>{globalStats.total_rows.toLocaleString()}</Typography>
        </Paper>
        <Paper sx={{ flex: 1, p: 2, borderRadius: 0, borderLeft: '4px solid #424242' }}>
          <Typography variant="caption" sx={{ color: '#757575', fontWeight: 'bold' }}>ACTIVE FLEET</Typography>
          <Typography variant="h5" sx={{ fontWeight: 'bold' }}>{globalStats.active_vehicles}</Typography>
        </Paper>
        <Paper sx={{ flex: 1, p: 2, borderRadius: 0, borderLeft: '4px solid #d32f2f' }}>
          <Typography variant="caption" sx={{ color: '#757575', fontWeight: 'bold' }}>DLQ BACKLOG</Typography>
          <Typography variant="h5" sx={{ fontWeight: 'bold', color: globalStats.dlq_backlog > 0 ? '#d32f2f' : '#212121' }}>{globalStats.dlq_backlog}</Typography>
        </Paper>
      </Box>

      {/* TAB CONTENT */}
      {activeTab === 'leaderboard' && (
        <Paper sx={{ flexGrow: 1, display: 'flex', flexDirection: 'column', borderRadius: 0, p: 0 }}>
          <Box className="ag-theme-balham" sx={{ flexGrow: 1, width: '100%' }}>
            <AgGridReact
              rowData={vehicles}
              columnDefs={columnDefs}
              animateRows={false}
              rowSelection="single"
              defaultColDef={{ resizable: true, sortable: true }}
              overlayLoadingTemplate={isLoading ? '<span class="ag-overlay-loading-center">Fetching Telemetry...</span>' : undefined}
            />
          </Box>
        </Paper>
      )}

      {activeTab === 'analytics' && (
        <Box sx={{ display: 'flex', gap: 2, flexGrow: 1 }}>
          <Paper sx={{ flex: 1, p: 2, borderRadius: 0 }}>
            <Typography variant="caption" sx={{ fontWeight: 'bold', color: '#616161', mb: 1 }}>LATENCY BY VEHICLE (ms)</Typography>
            <ResponsiveContainer width="100%" height="90%">
              <BarChart data={vehicles}>
                <CartesianGrid strokeDasharray="3 3" vertical={false} />
                <XAxis dataKey="vehicle_id" tick={{fontSize: 11}} />
                <YAxis />
                <Tooltip />
                <Bar dataKey="avg_latency" fill="#fbc02d" />
              </BarChart>
            </ResponsiveContainer>
          </Paper>
          <Paper sx={{ flex: 1, p: 2, borderRadius: 0 }}>
            <Typography variant="caption" sx={{ fontWeight: 'bold', color: '#616161', mb: 1 }}>DATA QUALITY DISTRIBUTION</Typography>
            <ResponsiveContainer width="100%" height="90%">
              <BarChart data={vehicles}>
                <CartesianGrid strokeDasharray="3 3" vertical={false} />
                <XAxis dataKey="vehicle_id" tick={{fontSize: 11}} />
                <YAxis />
                <Tooltip />
                <Legend />
                <Bar dataKey="rows_processed" name="Accepted" stackId="a" fill="#2e7d32" />
                <Bar dataKey="rejected_rows" name="Rejected" stackId="a" fill="#c62828" />
              </BarChart>
            </ResponsiveContainer>
          </Paper>
        </Box>
      )}

      {activeTab === 'inspector' && (
        <Paper sx={{ flexGrow: 1, display: 'flex', flexDirection: 'column', p: 2, borderRadius: 0 }}>
          <Box sx={{ display: 'flex', gap: 2, mb: 2 }}>
            <FormControl size="small" sx={{ minWidth: 200 }}>
              <InputLabel>Target Vehicle</InputLabel>
              <Select value={inspectorVid} onChange={(e) => { setInspectorVid(e.target.value); setInspectorSource('ALL (Latest)'); }} label="Target Vehicle">
                {vehicles.map((v: any) => <MenuItem key={v.vehicle_id} value={v.vehicle_id}>{v.vehicle_id}</MenuItem>)}
              </Select>
            </FormControl>
            <FormControl size="small" sx={{ minWidth: 200 }}>
              <InputLabel>Payload Source</InputLabel>
              <Select value={inspectorSource} onChange={(e) => setInspectorSource(e.target.value)} label="Payload Source">
                {availableInspectorSources.map((src: string) => <MenuItem key={src} value={src}>{src}</MenuItem>)}
              </Select>
            </FormControl>
          </Box>
          <Divider sx={{ mb: 2 }} />
          <Box sx={{ flexGrow: 1, bgcolor: '#1e1e1e', p: 2, overflow: 'auto' }}>
            <pre style={{ margin: 0, color: '#4caf50', fontFamily: 'monospace', fontSize: '13px' }}>
              {(() => {
                if (!selectedInspectorV) return "// NO DATA";
                const payload = inspectorSource === 'ALL (Latest)' 
                  ? selectedInspectorV.latest_payload 
                  : selectedInspectorV.module_payloads[inspectorSource];
                return payload ? JSON.stringify(payload, null, 2) : "// WAITING FOR PACKET...";
              })()}
            </pre>
          </Box>
        </Paper>
      )}

    </Box>
  );
}