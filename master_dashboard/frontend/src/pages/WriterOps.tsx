import React, { useMemo, useState, useEffect } from 'react';
import { 
  Box, Typography, Paper, Chip, Select, MenuItem, 
  FormControl, InputLabel, ToggleButton, ToggleButtonGroup, 
  Divider, Button 
} from '@mui/material';
import RefreshIcon from '@mui/icons-material/Refresh';
import { AgGridReact } from 'ag-grid-react';
import type { ColDef } from 'ag-grid-community';
import 'ag-grid-community/styles/ag-grid.css';
import 'ag-grid-community/styles/ag-theme-balham.css';
import { useQuery } from '@tanstack/react-query';
import axios from 'axios';
import { useStore } from '../store';

const fetchWriterMetrics = async () => {
  const { data } = await axios.get('http://127.0.0.1:8005/api/writer/metrics');
  return data;
};

const fetchInspectorData = async (module: string) => {
  const { data } = await axios.get(`http://127.0.0.1:8005/api/writer/inspector/${module}`);
  return data.data; 
};

export default function WriterOps() {
  const { autoRefresh } = useStore();
  
  const [viewMode, setViewMode] = useState<'operations' | 'inspector'>('operations');
  const [selectedModule, setSelectedModule] = useState<string>('engine');
  const [filterModule, setFilterModule] = useState<string>('ALL');

  const { data: metricsData, isLoading: metricsLoading, isError: metricsError } = useQuery({
    queryKey: ['writerMetrics'],
    queryFn: fetchWriterMetrics,
    refetchInterval: (viewMode === 'operations' && autoRefresh) ? 2000 : false,
  });

  const { data: inspectorData, isLoading: inspectorLoading, refetch: refetchInspector } = useQuery({
    queryKey: ['writerInspector', selectedModule],
    queryFn: () => fetchInspectorData(selectedModule),
    enabled: viewMode === 'inspector',
    refetchInterval: false, 
  });

  const metricsRowData = useMemo(() => {
    if (!metricsData) return [];
    try {
      let rows = Object.values(metricsData) as any[];
      if (filterModule !== 'ALL') {
        rows = rows.filter(r => r.module?.toLowerCase() === filterModule.toLowerCase());
      }
      return rows;
    } catch (e) {
      console.error("Error parsing metrics data:", e);
      return [];
    }
  }, [metricsData, filterModule]);

  const summaryStats = useMemo(() => {
    if (!metricsRowData.length) return { active: 0, written: 0, lag: 0, latency: 0 };
    const active = metricsRowData.filter(r => r.status === 'RUNNING').length;
    const written = metricsRowData.reduce((acc, r) => acc + (r.delta_total || 0), 0);
    const lag = metricsRowData.reduce((acc, r) => acc + (r.true_lag || 0), 0);
    const latencySum = metricsRowData.reduce((acc, r) => acc + (r.latency_ms || 0), 0);
    return { active, written, lag, latency: latencySum / metricsRowData.length };
  }, [metricsRowData]);

  const metricsColumnDefs = useMemo<ColDef[]>(() => [
    { field: 'module', headerName: 'SUBSYSTEM', sortable: true, filter: true, width: 150 },
    { 
      field: 'status', 
      headerName: 'PROCESS STATUS', 
      width: 160,
      cellRenderer: (params: any) => {
        let color: "success" | "error" | "warning" = "error";
        if (params.value === 'RUNNING') color = "success";
        if (params.value === 'STALLED') color = "warning";
        return <Chip label={params.value || 'UNKNOWN'} color={color} size="small" sx={{ borderRadius: '2px', height: '20px', fontSize: '0.75rem', fontWeight: 'bold' }} />;
      }
    },
    { field: 'kafka_total', headerName: 'KAFKA OFFSET', width: 140, type: 'numericColumn' },
    { field: 'delta_total', headerName: 'DELTA RECORDS', width: 140, type: 'numericColumn' },
    { field: 'true_lag', headerName: 'SYSTEM LAG', width: 140, type: 'numericColumn' },
    { field: 'throughput', headerName: 'IN RATE', width: 130, type: 'numericColumn' },
    { field: 'processed', headerName: 'OUT RATE', width: 130, type: 'numericColumn' },
    { field: 'latency_ms', headerName: 'LATENCY', flex: 1, type: 'numericColumn' },
  ], []);

  const inspectorColumnDefs = useMemo<ColDef[]>(() => {
    if (!inspectorData || inspectorData.length === 0) return [];
    return Object.keys(inspectorData[0]).map(key => ({
      field: key,
      headerName: key.toUpperCase(),
      sortable: true,
      filter: true,
      width: key.includes('ts') || key.includes('hash') ? 220 : 130,
    }));
  }, [inspectorData]);

  // --- DEBUGGING LOGS ---
  useEffect(() => {
    console.log("=== WRITER OPS DEBUG ===");
    console.log("1. Raw API Metrics Data:", metricsData);
    console.log("2. Parsed Metrics Row Data:", metricsRowData);
    console.log("3. Raw Inspector Data:", inspectorData);
    console.log("4. Inspector Columns:", inspectorColumnDefs);
  }, [metricsData, metricsRowData, inspectorData, inspectorColumnDefs]);

  return (
    <Box sx={{ p: 2, bgcolor: '#f5f5f5', minHeight: '100vh' }}>
      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-end', borderBottom: '2px solid #bdbdbd', pb: 1, mb: 2 }}>
        <Typography variant="h5" sx={{ fontWeight: 700, color: '#212121', letterSpacing: '-0.5px' }}>
          BRONZE LAYER WRITER PIPELINE
        </Typography>
        <ToggleButtonGroup value={viewMode} exclusive onChange={(e, val) => val && setViewMode(val)} size="small" sx={{ bgcolor: 'white' }}>
          <ToggleButton value="operations" sx={{ fontWeight: 'bold', px: 3, borderRadius: 0 }}>OPERATIONS METRICS</ToggleButton>
          <ToggleButton value="inspector" sx={{ fontWeight: 'bold', px: 3, borderRadius: 0 }}>DATA INSPECTOR</ToggleButton>
        </ToggleButtonGroup>
      </Box>

      {viewMode === 'operations' && (
        <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
          <Box sx={{ display: 'flex', gap: 2, width: '100%' }}>
            {[
              { label: 'ACTIVE WRITERS', value: `${summaryStats.active} / 5` },
              { label: 'TOTAL WRITTEN', value: summaryStats.written.toLocaleString() },
              { label: 'GLOBAL LAG', value: summaryStats.lag.toLocaleString(), color: summaryStats.lag > 500 ? '#d32f2f' : '#2e7d32' },
              { label: 'AVG LATENCY', value: `${summaryStats.latency.toFixed(1)} ms` }
            ].map((kpi, idx) => (
              <Paper key={idx} sx={{ flex: 1, p: 2, borderRadius: 0, borderLeft: '4px solid #424242' }}>
                <Typography variant="caption" sx={{ color: '#757575', fontWeight: 'bold' }}>{kpi.label}</Typography>
                <Typography variant="h5" sx={{ fontWeight: 'bold', color: kpi.color || '#212121', mt: 0.5 }}>{kpi.value}</Typography>
              </Paper>
            ))}
          </Box>

          <Paper sx={{ p: 1, borderRadius: 0, display: 'flex', alignItems: 'center', gap: 2 }}>
            <Typography variant="body2" sx={{ fontWeight: 'bold', ml: 1 }}>FILTER CONTEXT:</Typography>
            <FormControl size="small" sx={{ minWidth: 200 }}>
              <InputLabel>Subsystem Module</InputLabel>
              <Select value={filterModule} onChange={(e) => setFilterModule(e.target.value)} label="Subsystem Module" sx={{ borderRadius: 0 }}>
                <MenuItem value="ALL">ALL MODULES</MenuItem>
                <MenuItem value="BATTERY">BATTERY</MenuItem>
                <MenuItem value="BODY">BODY</MenuItem>
                <MenuItem value="ENGINE">ENGINE</MenuItem>
                <MenuItem value="TRANSMISSION">TRANSMISSION</MenuItem>
                <MenuItem value="TYRE">TYRE</MenuItem>
              </Select>
            </FormControl>
          </Paper>

          {/* HARDCODED STYLE OVERRIDE FOR AG GRID */}
          <Paper sx={{ p: 0, borderRadius: 0 }}>
            <div className="ag-theme-balham" style={{ height: '500px', width: '100%' }}>
              <AgGridReact
                rowData={metricsRowData}
                columnDefs={metricsColumnDefs}
                animateRows={false} 
                rowSelection="single"
                defaultColDef={{ resizable: true, sortable: true }}
                overlayLoadingTemplate={metricsLoading ? '<span class="ag-overlay-loading-center">Fetching Telemetry...</span>' : undefined}
              />
            </div>
          </Paper>
        </Box>
      )}

      {viewMode === 'inspector' && (
        <Paper sx={{ display: 'flex', flexDirection: 'column', borderRadius: 0, p: 2 }}>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 2, mb: 2 }}>
             <FormControl size="small" sx={{ minWidth: 250 }}>
              <InputLabel>Target Parquet Module</InputLabel>
              <Select value={selectedModule} onChange={(e) => setSelectedModule(e.target.value)} label="Target Parquet Module" sx={{ borderRadius: 0 }}>
                <MenuItem value="battery">BATTERY</MenuItem>
                <MenuItem value="body">BODY</MenuItem>
                <MenuItem value="engine">ENGINE</MenuItem>
                <MenuItem value="transmission">TRANSMISSION</MenuItem>
                <MenuItem value="tyre">TYRE</MenuItem>
              </Select>
            </FormControl>
            <Button variant="contained" color="primary" onClick={() => refetchInspector()} disabled={inspectorLoading} startIcon={<RefreshIcon />} sx={{ borderRadius: 0, fontWeight: 'bold', height: '40px', boxShadow: 'none' }}>
              FETCH LATEST ROWS
            </Button>
          </Box>
          <Divider sx={{ mb: 2 }} />

          {/* HARDCODED STYLE OVERRIDE FOR AG GRID */}
          <div className="ag-theme-balham" style={{ height: '600px', width: '100%' }}>
            <AgGridReact
              rowData={inspectorData || []}
              columnDefs={inspectorColumnDefs}
              defaultColDef={{ resizable: true, sortable: true, filter: true }}
              overlayLoadingTemplate={inspectorLoading ? '<span class="ag-overlay-loading-center">Scanning Parquet...</span>' : undefined}
            />
          </div>
        </Paper>
      )}
    </Box>
  );
}