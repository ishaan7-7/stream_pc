import React, { useMemo, useState, useEffect } from 'react';
import { 
  Box, Typography, Paper, Select, MenuItem, FormControl, InputLabel, 
  ToggleButton, ToggleButtonGroup, Divider, Checkbox, FormControlLabel, 
  Slider, Button, Alert, Chip
} from '@mui/material';
import { AgGridReact } from 'ag-grid-react';
import type { ColDef } from 'ag-grid-community';
import { ModuleRegistry, AllCommunityModule } from 'ag-grid-community';
import 'ag-grid-community/styles/ag-grid.css';
import 'ag-grid-community/styles/ag-theme-balham.css';
import { useQuery } from '@tanstack/react-query';
import axios from 'axios';
import { useStore } from '../store';
import { 
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer 
} from 'recharts';
import AutoFixHighIcon from '@mui/icons-material/AutoFixHigh';

ModuleRegistry.registerModules([AllCommunityModule]);

const ALL_MODULES = ["engine", "transmission", "battery", "body", "tyre"];
const COLORS = { engine: '#e57373', transmission: '#ffb74d', battery: '#81c784', body: '#ba68c8', tyre: '#4dd0e1' };

const fetchGoldMetrics = async () => (await axios.get('http://127.0.0.1:8005/api/gold/metrics')).data;
const fetchGoldConfig = async () => (await axios.get('http://127.0.0.1:8005/api/gold/config')).data;
const fetchGoldHistory = async (simId: string) => (await axios.get(`http://127.0.0.1:8005/api/gold/history/${simId}`)).data.data;

export default function GoldHealth() {
  const { autoRefresh } = useStore();
  
  const [activeTab, setActiveTab] = useState<'operations' | 'experiment'>('operations');
  const [selectedSim, setSelectedSim] = useState<string>('sim001');

  // Experimentation State
  const [activeModules, setActiveModules] = useState<string[]>([]);
  const [expWeights, setExpWeights] = useState<Record<string, number>>({});

  // Queries
  const { data: metrics } = useQuery({ queryKey: ['goldMetrics'], queryFn: fetchGoldMetrics, refetchInterval: autoRefresh ? 2000 : false });
  const { data: config } = useQuery({ queryKey: ['goldConfig'], queryFn: fetchGoldConfig, refetchInterval: false });
  
  const availableSims = useMemo(() => metrics?.active_sims || ['sim001'], [metrics]);

  const { data: history } = useQuery({ 
    queryKey: ['goldHistory', selectedSim], 
    queryFn: () => fetchGoldHistory(selectedSim),
    refetchInterval: (activeTab === 'operations' && autoRefresh) ? 2000 : false,
  });

  // Initialize Experimentation state from backend config once loaded
  useEffect(() => {
    if (config && activeModules.length === 0) {
      setActiveModules(config.enabled_modules || ALL_MODULES);
      setExpWeights(config.default_weights || {});
    }
  }, [config]);

  // --- EXPERIMENTATION CALIBRATION LOGIC ---
  const handleToggleModule = (mod: string) => {
    setActiveModules(prev => prev.includes(mod) ? prev.filter(m => m !== mod) : [...prev, mod]);
  };

  const handleWeightChange = (mod: string, val: number) => {
    setExpWeights(prev => ({ ...prev, [mod]: val }));
  };

  const currentWeightSum = useMemo(() => {
    return activeModules.reduce((sum, mod) => sum + (expWeights[mod] || 0), 0);
  }, [activeModules, expWeights]);

  const isUnbalanced = Math.abs(currentWeightSum - 1.0) > 0.001;

  const autoBalanceWeights = () => {
    if (currentWeightSum === 0) return;
    const balanced: Record<string, number> = {};
    activeModules.forEach(mod => {
      balanced[mod] = parseFloat(((expWeights[mod] || 0) / currentWeightSum).toFixed(3));
    });
    setExpWeights(prev => ({ ...prev, ...balanced }));
  };

  // --- MATHEMATICAL RE-FUSION ENGINE (Runs entirely in browser memory) ---
  const simulatedHistory = useMemo(() => {
    if (!history || !config) return [];
    
    return history.map((row: any) => {
      let simulatedHealth = 0;
      let minPenaltyHealth = 100;
      const parsedRow: any = { ...row, ts_short: row.gold_window_ts.split(' ')[1] || row.gold_window_ts };

      activeModules.forEach(mod => {
        const contribKey = `${mod}_contrib`;
        const defaultWeight = config.default_weights[mod] || 0.2;
        const currentContrib = row[contribKey] || 0;

        // Reverse-engineer the raw module health from the backend contrib
        const rawHealth = defaultWeight > 0 ? (currentContrib / defaultWeight) : 100;
        parsedRow[`${mod}_raw`] = rawHealth; // Save for background plotting

        // Apply user's experimental weight
        const customWeight = expWeights[mod] || 0;
        simulatedHealth += (rawHealth * customWeight);

        // Check backend penalties
        const penaltyThresh = config.tier_1_penalties[mod];
        if (penaltyThresh && rawHealth < penaltyThresh) {
          minPenaltyHealth = Math.min(minPenaltyHealth, rawHealth);
        }
      });

      // Clamp if critical penalty triggered
      if (minPenaltyHealth < 100 && minPenaltyHealth < simulatedHealth) {
        simulatedHealth = minPenaltyHealth;
        parsedRow.penalty_active = true;
      } else {
        parsedRow.penalty_active = false;
      }

      parsedRow.experimental_health = parseFloat(simulatedHealth.toFixed(2));
      return parsedRow;
    });
  }, [history, config, activeModules, expWeights]);

  // --- OPERATIONS BOARD DATA ---
  const latestRow = history && history.length > 0 ? history[history.length - 1] : null;
  const topFeatures = useMemo(() => {
    if (!latestRow || !latestRow.top_5_features) return [];
    try {
      const obj = JSON.parse(latestRow.top_5_features);
      return Object.entries(obj).map(([k, v]) => ({ feature: k, impact: v }));
    } catch { return []; }
  }, [latestRow]);

  const tableColDefs = useMemo<ColDef[]>(() => {
    if (!history || !history[0]) return [];
    return Object.keys(history[0])
      .filter(k => k !== 'top_5_features') // Hide messy json
      .map(key => ({
        field: key,
        headerName: key.toUpperCase(),
        sortable: true,
        filter: true,
        width: key.includes('ts') ? 200 : 130,
        cellStyle: key === 'vehicle_health_score' ? (params: any) => ({
          fontWeight: 'bold', 
          color: params.value < 50 ? '#d32f2f' : (params.value < 80 ? '#f57c00' : '#388e3c')
        }) : undefined
      }));
  }, [history]);

  const chartAxisStyle = { fontSize: '11px', fill: '#616161', fontWeight: 600 };

  return (
    <Box sx={{ height: 'calc(100vh - 80px)', display: 'flex', flexDirection: 'column', gap: 2, p: 2, bgcolor: '#f5f5f5' }}>
      
      {/* HEADER */}
      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-end', borderBottom: '2px solid #bdbdbd', pb: 1 }}>
        <Typography variant="h5" sx={{ fontWeight: 700, color: '#212121', letterSpacing: '-0.5px' }}>
          GOLD LAYER: FUSED VEHICLE HEALTH
        </Typography>
        <Box sx={{ display: 'flex', gap: 2, alignItems: 'center' }}>
          <FormControl size="small" sx={{ minWidth: 150, bgcolor: 'white' }}>
            <InputLabel>Active Vehicle</InputLabel>
            <Select value={selectedSim} onChange={(e) => setSelectedSim(e.target.value)} label="Active Vehicle" sx={{ borderRadius: 0, height: '35px' }}>
              {availableSims.map((sim: string) => <MenuItem key={sim} value={sim}>{sim}</MenuItem>)}
            </Select>
          </FormControl>
          <ToggleButtonGroup value={activeTab} exclusive onChange={(e, val) => val && setActiveTab(val)} size="small" sx={{ bgcolor: 'white' }}>
            <ToggleButton value="operations" sx={{ fontWeight: 'bold', px: 3, borderRadius: 0 }}>LIVE OPERATIONS</ToggleButton>
            <ToggleButton value="experiment" sx={{ fontWeight: 'bold', px: 3, borderRadius: 0 }}>WEIGHT EXPERIMENTATION LAB</ToggleButton>
          </ToggleButtonGroup>
        </Box>
      </Box>

      {/* TAB 1: LIVE OPERATIONS (Matches Streamlit) */}
      {activeTab === 'operations' && (
        <Box sx={{ display: 'flex', flexDirection: 'column', height: '100%', gap: 2 }}>
          
          <Box sx={{ display: 'flex', gap: 2, width: '100%' }}>
            {[
              { label: 'ACTIVE SIMULATIONS', value: availableSims.length },
              { label: 'TOTAL GOLD ROWS', value: metrics?.total_gold_rows?.toLocaleString() || 0 },
              { label: `PROCESSING LAG (vs ${metrics?.primary_module?.toUpperCase() || 'RAW'})`, value: metrics?.processing_lag?.toLocaleString() || 0, color: (metrics?.processing_lag || 0) > 1000 ? '#d32f2f' : '#212121' },
              { label: 'CURRENT HEALTH (LATEST)', value: `${latestRow?.vehicle_health_score || 0}%`, color: (latestRow?.vehicle_health_score || 100) < 60 ? '#d32f2f' : '#2e7d32' }
            ].map((kpi, idx) => (
              <Paper key={idx} sx={{ flex: 1, p: 2, borderRadius: 0, borderLeft: '4px solid #fbc02d' }}>
                <Typography variant="caption" sx={{ color: '#757575', fontWeight: 'bold' }}>{kpi.label}</Typography>
                <Typography variant="h5" sx={{ fontWeight: 'bold', color: kpi.color || '#212121', mt: 0.5 }}>{kpi.value}</Typography>
              </Paper>
            ))}
          </Box>

          <Box sx={{ display: 'flex', gap: 2, flex: 1, minHeight: 0 }}>
            <Paper sx={{ width: '300px', p: 2, borderRadius: 0, display: 'flex', flexDirection: 'column', overflowY: 'auto' }}>
              <Typography variant="caption" sx={{ fontWeight: 'bold', color: '#616161', mb: 2 }}>LATEST ANOMALY DRIVERS</Typography>
              {topFeatures.length > 0 ? topFeatures.map((f: any, i) => (
                <Box key={i} sx={{ mb: 1.5 }}>
                  <Typography variant="body2" sx={{ fontWeight: 'bold', fontSize: '12px' }}>{f.feature}</Typography>
                  <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                    <Box sx={{ flex: 1, height: '6px', bgcolor: '#eeeeee', borderRadius: '3px', overflow: 'hidden' }}>
                      <Box sx={{ width: `${Math.min(100, (f.impact as number) * 10)}%`, height: '100%', bgcolor: '#d32f2f' }} />
                    </Box>
                    <Typography variant="caption">{parseFloat(f.impact).toFixed(2)}</Typography>
                  </Box>
                </Box>
              )) : <Typography variant="caption">Awaiting anomaly data...</Typography>}
            </Paper>

            <Paper sx={{ flex: 1, display: 'flex', flexDirection: 'column', p: 0, borderRadius: 0 }}>
              <Box sx={{ p: 1, borderBottom: '1px solid #e0e0e0', bgcolor: '#fafafa' }}>
                 <Typography variant="body2" sx={{ fontWeight: 'bold', ml: 1, color: '#424242' }}>LATEST {history?.length || 0} RECORDS: {selectedSim}</Typography>
              </Box>
              <Box className="ag-theme-balham" sx={{ flexGrow: 1, width: '100%' }}>
                <AgGridReact
                  rowData={history ? [...history].reverse() : []}
                  columnDefs={tableColDefs}
                  defaultColDef={{ resizable: true, sortable: true }}
                />
              </Box>
            </Paper>
          </Box>
        </Box>
      )}

      {/* TAB 2: WEIGHT EXPERIMENTATION LAB */}
      {activeTab === 'experiment' && (
        <Box sx={{ display: 'flex', gap: 2, height: '100%' }}>
          
          {/* LEFT SIDEBAR: CONTROL PANEL */}
          <Paper sx={{ width: '320px', p: 2, borderRadius: 0, display: 'flex', flexDirection: 'column', overflowY: 'auto', bgcolor: 'white' }}>
            <Typography variant="subtitle2" sx={{ fontWeight: 800, color: '#212121', mb: 2 }}>DYNAMIC WEIGHT CONFIG</Typography>
            
            {isUnbalanced && (
              <Alert 
                severity="warning" 
                icon={false}
                sx={{ borderRadius: 0, mb: 2, '& .MuiAlert-message': { p: 0 } }}
                action={
                  <Button size="small" color="inherit" onClick={autoBalanceWeights} startIcon={<AutoFixHighIcon />}>
                    RECALIBRATE
                  </Button>
                }
              >
                <Typography variant="caption" sx={{ fontWeight: 'bold', display: 'block' }}>
                  Sum is {currentWeightSum.toFixed(3)} (Target: 1.0)
                </Typography>
              </Alert>
            )}

            <Box sx={{ display: 'flex', flexDirection: 'column', gap: 3 }}>
              {ALL_MODULES.map(mod => {
                const isActive = activeModules.includes(mod);
                return (
                  <Box key={mod} sx={{ opacity: isActive ? 1 : 0.5 }}>
                    <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                      <FormControlLabel
                        control={<Checkbox size="small" checked={isActive} onChange={() => handleToggleModule(mod)} sx={{ color: COLORS[mod as keyof typeof COLORS] }} />}
                        label={<Typography variant="body2" sx={{ fontWeight: 'bold', textTransform: 'uppercase' }}>{mod}</Typography>}
                      />
                      <Chip size="small" label={isActive ? (expWeights[mod] || 0).toFixed(2) : 'OFF'} sx={{ borderRadius: 1, height: '20px', fontSize: '11px', fontWeight: 'bold' }} />
                    </Box>
                    <Slider 
                      size="small"
                      value={expWeights[mod] || 0} 
                      onChange={(_, val) => handleWeightChange(mod, val as number)}
                      min={0} max={1} step={0.01}
                      disabled={!isActive}
                      sx={{ color: COLORS[mod as keyof typeof COLORS], ml: 4, width: 'calc(100% - 32px)', mt: -1 }}
                    />
                  </Box>
                );
              })}
            </Box>
          </Paper>

          {/* RIGHT AREA: MASSIVE DYNAMIC GRAPH */}
          <Paper sx={{ flex: 1, p: 2, borderRadius: 0, display: 'flex', flexDirection: 'column' }}>
            <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 2 }}>
              <Typography variant="subtitle2" sx={{ fontWeight: 800, color: '#212121' }}>RECALCULATED HEALTH TRAJECTORY</Typography>
              {simulatedHistory[simulatedHistory.length - 1]?.penalty_active && (
                <Chip label="CRITICAL PENALTY CLAMP ACTIVE" color="error" size="small" sx={{ borderRadius: 0, fontWeight: 'bold' }} />
              )}
            </Box>
            
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={simulatedHistory} margin={{ top: 10, right: 30, left: -20, bottom: 0 }}>
                <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#eeeeee" />
                <XAxis dataKey="ts_short" tick={chartAxisStyle} axisLine={{ stroke: '#bdbdbd' }} tickLine={false} minTickGap={30} />
                <YAxis domain={[0, 100]} tick={chartAxisStyle} axisLine={{ stroke: '#bdbdbd' }} tickLine={false} />
                <Tooltip contentStyle={{ borderRadius: 0, fontSize: '12px', padding: '10px' }} />
                <Legend wrapperStyle={{ fontSize: '12px', fontWeight: 'bold', color: '#616161' }} />
                
                {/* Background Raw Module Scores (Faded) */}
                {activeModules.map(mod => (
                  <Line 
                    key={`${mod}_raw`} type="monotone" dataKey={`${mod}_raw`} name={`${mod.toUpperCase()} (Raw)`} 
                    stroke={COLORS[mod as keyof typeof COLORS]} strokeWidth={1} strokeDasharray="5 5" dot={false} 
                  />
                ))}

                {/* Primary Thick Health Curve */}
                <Line 
                  type="monotone" dataKey="experimental_health" name="FUSED VEHICLE HEALTH" 
                  stroke="#1976d2" strokeWidth={4} dot={false} activeDot={{ r: 8 }}
                />
              </LineChart>
            </ResponsiveContainer>
          </Paper>
        </Box>
      )}
    </Box>
  );
}