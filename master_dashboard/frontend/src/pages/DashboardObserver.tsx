import React, { useState, useMemo } from 'react';
import { 
  Box, Typography, Paper, ToggleButtonGroup, ToggleButton, Chip, Divider, 
  Select, MenuItem, FormControl, InputLabel 
} from '@mui/material';
import { AgGridReact } from 'ag-grid-react';
import type { ColDef } from 'ag-grid-community';
import { useQuery } from '@tanstack/react-query';
import axios from 'axios';
import { useStore } from '../store';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';

// --- API Fetcher ---
const fetchObserverSnapshot = async () => {
  const { data } = await axios.get('http://127.0.0.1:8005/api/observer/snapshot');
  return data;
};

export default function DashboardObserver() {
  const { autoRefresh } = useStore();
  const [activeTab, setActiveTab] = useState<'fleet_status' | 'inspector'>('fleet_status');
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

  const columnDefs = useMemo<ColDef[]>(() => [
    { field: 'vehicle_id', headerName: 'VEHICLE ID', flex: 1, minWidth: 150, cellStyle: { fontWeight: 'bold', color: '#1976d2' } },
    { field: 'rows_processed', headerName: 'PROCESSED', flex: 1, type: 'numericColumn', valueFormatter: p => p.value?.toLocaleString() },
    { field: 'rejected_rows', headerName: 'REJECTED', flex: 1, type: 'numericColumn', cellStyle: { color: '#d32f2f' } },
    { 
      field: 'validation_rate', 
      headerName: 'QUALITY SCORE', 
      flex: 1, 
      valueFormatter: p => p.value != null ? `${p.value.toFixed(1)}%` : '',
      cellStyle: (p) => ({ color: p.value > 95 ? '#2e7d32' : '#d32f2f', fontWeight: 'bold' })
    },
    { field: 'avg_latency', headerName: 'LATENCY (ms)', flex: 1, type: 'numericColumn', valueFormatter: p => p.value?.toFixed(1) },
    { field: 'last_seen_sec', headerName: 'LAST SEEN (s)', flex: 1, type: 'numericColumn', valueFormatter: p => p.value != null ? `${p.value.toFixed(1)}s ago` : '' },
  ], []);

  // Filter out non-relevant health metrics
  const filteredHealth = Object.entries(systemHealth).filter(
    ([name]) => !['kafka_metrics', 'dashboard'].includes(name)
  );

  return (
    <Box sx={{ height: 'calc(100vh - 80px)', display: 'flex', flexDirection: 'column', gap: 2, p: 2, bgcolor: '#f5f5f5' }}>
      
      {/* HEADER & TABS */}
      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-end', borderBottom: '2px solid #bdbdbd', pb: 1 }}>
        <Typography variant="h5" sx={{ fontWeight: 700, color: '#212121', letterSpacing: '-0.5px' }}>
          TELEMETRY OBSERVER
        </Typography>
        <ToggleButtonGroup value={activeTab} exclusive onChange={(e, val) => val && setActiveTab(val)} size="small" sx={{ bgcolor: 'white' }}>
          <ToggleButton value="fleet_status" sx={{ fontWeight: 'bold', px: 3, borderRadius: 0 }}>FLEET STATUS</ToggleButton>
          <ToggleButton value="inspector" sx={{ fontWeight: 'bold', px: 3, borderRadius: 0 }}>LIVE INSPECTOR</ToggleButton>
        </ToggleButtonGroup>
      </Box>

      {/* HEALTH & KPIs */}
      <Box sx={{ display: 'flex', gap: 2, width: '100%' }}>
        <Paper sx={{ p: 2, borderRadius: 0, display: 'flex', gap: 2, flexWrap: 'wrap', flex: 1.5, borderLeft: '4px solid #1976d2' }}>
          {filteredHealth.map(([name, isUp]) => (
            <Chip key={name} label={name.toUpperCase()} color={isUp ? "success" : "error"} size="small" variant={isUp ? "outlined" : "filled"} sx={{ borderRadius: '2px', fontWeight: 'bold' }} />
          ))}
          {filteredHealth.length === 0 && <Typography variant="caption" color="textSecondary">AWAITING HEALTH DATA</Typography>}
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
      {activeTab === 'fleet_status' && (
        <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2, flexGrow: 1, minHeight: 0 }}>
          {/* Data Grid Section */}
          <Paper sx={{ flex: 1, display: 'flex', flexDirection: 'column', borderRadius: 0, p: 0, minHeight: '200px' }}>
            <Box className="ag-theme-balham" sx={{ flexGrow: 1, width: '100%' }}>
              <AgGridReact
                rowData={vehicles}
                columnDefs={columnDefs}
                animateRows={false}
                rowSelection="single"
                defaultColDef={{ resizable: true, sortable: true }}
                overlayLoadingTemplate={isLoading && vehicles.length === 0 ? '<span class="ag-overlay-loading-center">Fetching Telemetry...</span>' : undefined}
                overlayNoRowsTemplate={isError ? '<span class="ag-overlay-loading-center">ERROR: Backend Unreachable</span>' : '<span class="ag-overlay-loading-center">No Fleet Data Available</span>'}
              />
            </Box>
          </Paper>

          {/* Bottom Analytics Visualizations Section */}
          <Box sx={{ display: 'flex', gap: 2, height: '240px' }}>
            <Paper sx={{ flex: 1, p: 2, borderRadius: 0, display: 'flex', flexDirection: 'column' }}>
              <Typography variant="caption" sx={{ fontWeight: 'bold', color: '#616161', mb: 1 }}>LATENCY BY VEHICLE (ms)</Typography>
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={vehicles} margin={{ top: 10, right: 10, left: -20, bottom: 0 }}>
                  <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#eeeeee" />
                  <XAxis dataKey="vehicle_id" tick={{fontSize: 11, fill: '#616161'}} axisLine={{ stroke: '#bdbdbd' }} tickLine={false} />
                  <YAxis tick={{fontSize: 11, fill: '#616161'}} axisLine={{ stroke: '#bdbdbd' }} tickLine={false} />
                  <Tooltip cursor={{ fill: '#f5f5f5' }} contentStyle={{ borderRadius: 0, fontSize: '12px', padding: '5px' }} />
                  <Bar dataKey="avg_latency" fill="#fbc02d" barSize={30} />
                </BarChart>
              </ResponsiveContainer>
            </Paper>

            <Paper sx={{ flex: 1, p: 2, borderRadius: 0, display: 'flex', flexDirection: 'column' }}>
              <Typography variant="caption" sx={{ fontWeight: 'bold', color: '#616161', mb: 1 }}>DATA QUALITY DISTRIBUTION</Typography>
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={vehicles} margin={{ top: 10, right: 10, left: -20, bottom: 0 }}>
                  <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#eeeeee" />
                  <XAxis dataKey="vehicle_id" tick={{fontSize: 11, fill: '#616161'}} axisLine={{ stroke: '#bdbdbd' }} tickLine={false} />
                  <YAxis tick={{fontSize: 11, fill: '#616161'}} axisLine={{ stroke: '#bdbdbd' }} tickLine={false} />
                  <Tooltip cursor={{ fill: '#f5f5f5' }} contentStyle={{ borderRadius: 0, fontSize: '12px', padding: '5px' }} />
                  <Legend wrapperStyle={{ fontSize: '12px' }} />
                  <Bar dataKey="rows_processed" name="Accepted" stackId="a" fill="#2e7d32" barSize={30} />
                  <Bar dataKey="rejected_rows" name="Rejected" stackId="a" fill="#c62828" barSize={30} />
                </BarChart>
              </ResponsiveContainer>
            </Paper>
          </Box>
        </Box>
      )}

      {activeTab === 'inspector' && (
        <Paper sx={{ flexGrow: 1, display: 'flex', flexDirection: 'column', p: 2, borderRadius: 0, minHeight: 0 }}>
          <Box sx={{ display: 'flex', gap: 2, mb: 2 }}>
            <FormControl size="small" sx={{ minWidth: 200, bgcolor: 'white' }}>
              <InputLabel>Target Vehicle</InputLabel>
              <Select value={inspectorVid} onChange={(e) => { setInspectorVid(e.target.value); setInspectorSource('ALL (Latest)'); }} label="Target Vehicle" sx={{ borderRadius: 0 }}>
                {vehicles.map((v: any) => <MenuItem key={v.vehicle_id} value={v.vehicle_id}>{v.vehicle_id}</MenuItem>)}
              </Select>
            </FormControl>
            <FormControl size="small" sx={{ minWidth: 200, bgcolor: 'white' }}>
              <InputLabel>Payload Source</InputLabel>
              <Select value={inspectorSource} onChange={(e) => setInspectorSource(e.target.value)} label="Payload Source" sx={{ borderRadius: 0 }}>
                {availableInspectorSources.map((src: string) => <MenuItem key={src} value={src}>{src}</MenuItem>)}
              </Select>
            </FormControl>
          </Box>
          <Divider sx={{ mb: 2 }} />
          <Box sx={{ flexGrow: 1, bgcolor: '#fafafa', border: '1px solid #e0e0e0', p: 2, overflow: 'auto' }}>
            <pre style={{ margin: 0, color: '#37474f', fontFamily: 'Consolas, monospace', fontSize: '13px', whiteSpace: 'pre-wrap', wordWrap: 'break-word' }}>
              {(() => {
                if (!selectedInspectorV) return "// NO DATA FOR TARGET VEHICLE";
                const payload = inspectorSource === 'ALL (Latest)' 
                  ? selectedInspectorV.latest_payload 
                  : selectedInspectorV.module_payloads?.[inspectorSource];
                return payload ? JSON.stringify(payload, null, 2) : "// WAITING FOR PACKET...";
              })()}
            </pre>
          </Box>
        </Paper>
      )}

    </Box>
  );
}