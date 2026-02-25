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
  
  // UNIFIED FILTER CONTEXT STATE
  const [filterSim, setFilterSim] = useState<string>('ALL');
  const [filterModule, setFilterModule] = useState<string>('ALL');

  // Experimentation State
  const [activeModules, setActiveModules] = useState<string[]>([]);
  const [expWeights, setExpWeights] = useState<Record<string, number>>({});

  // Queries
  const { data: metrics } = useQuery({ queryKey: ['goldMetrics'], queryFn: fetchGoldMetrics, refetchInterval: autoRefresh ? 2000 : false });
  const { data: config } = useQuery({ queryKey: ['goldConfig'], queryFn: fetchGoldConfig, refetchInterval: false });
  
  const availableSims = useMemo(() => metrics?.active_sims || [], [metrics]);

  const { data: history } = useQuery({ 
    queryKey: ['goldHistory', filterSim], 
    queryFn: () => fetchGoldHistory(filterSim),
    refetchInterval: (activeTab === 'operations' && autoRefresh) ? 2000 : false,
  });

  useEffect(() => {
    if (config && activeModules.length === 0) {
      setActiveModules(config.enabled_modules || ALL_MODULES);
      setExpWeights(config.default_weights || {});
    }
  }, [config]);

  // --- KPI LOGIC ---
  const displayLag = useMemo(() => {
    if (!metrics || !metrics.processing_lags) return 0;
    if (filterModule === 'ALL') {
      return Math.max(...Object.values(metrics.processing_lags as Record<string, number>));
    }
    return metrics.processing_lags[filterModule.toLowerCase()] || 0;
  }, [metrics, filterModule]);

  // --- EXPERIMENTATION CALIBRATION LOGIC ---
  const handleToggleModule = (mod: string) => {
    setActiveModules(prev => prev.includes(mod) ? prev.filter(m => m !== mod) : [...prev, mod]);
  };
  const handleWeightChange = (mod: string, val: number) => {
    setExpWeights(prev => ({ ...prev, [mod]: val }));
  };
  const currentWeightSum = useMemo(() => activeModules.reduce((sum, mod) => sum + (expWeights[mod] || 0), 0), [activeModules, expWeights]);
  const isUnbalanced = Math.abs(currentWeightSum - 1.0) > 0.001;

  const autoBalanceWeights = () => {
    if (currentWeightSum === 0) return;
    const balanced: Record<string, number> = {};
    activeModules.forEach(mod => {
      balanced[mod] = parseFloat(((expWeights[mod] || 0) / currentWeightSum).toFixed(3));
    });
    setExpWeights(prev => ({ ...prev, ...balanced }));
  };

  // --- MATHEMATICAL RE-FUSION ENGINE & FLEET AGGREGATION ---
  const simulatedHistory = useMemo(() => {
    if (!history || !config) return [];
    
    // Step 1: Calculate raw reverse-engineered health for every row
    const processedRows = history.map((row: any) => {
      let simulatedHealth = 0;
      let minPenaltyHealth = 100;
      const parsedRow: any = { ...row, ts_short: row.gold_window_ts.split(' ')[1] || row.gold_window_ts };

      activeModules.forEach(mod => {
        const contribKey = `${mod}_contrib`;
        const defaultWeight = config.default_weights[mod] || 0.2;
        const currentContrib = row[contribKey] || 0;
        const rawHealth = defaultWeight > 0 ? (currentContrib / defaultWeight) : 100;
        parsedRow[`${mod}_raw`] = rawHealth; 

        const customWeight = expWeights[mod] || 0;
        simulatedHealth += (rawHealth * customWeight);

        const penaltyThresh = config.tier_1_penalties[mod];
        if (penaltyThresh && rawHealth < penaltyThresh) {
          minPenaltyHealth = Math.min(minPenaltyHealth, rawHealth);
        }
      });

      if (minPenaltyHealth < 100 && minPenaltyHealth < simulatedHealth) {
        simulatedHealth = minPenaltyHealth;
        parsedRow.penalty_active = true;
      } else {
        parsedRow.penalty_active = false;
      }
      parsedRow.experimental_health = parseFloat(simulatedHealth.toFixed(2));
      return parsedRow;
    });

    // Step 2: If "ALL" vehicles are selected, we must group by timestamp and average the healths
    if (filterSim === 'ALL') {
      const grouped: Record<string, any> = {};
      processedRows.forEach((row: any) => {
        if (!grouped[row.ts_short]) {
          grouped[row.ts_short] = { count: 0, experimental_health: 0, penalty_active: false };
          activeModules.forEach(m => grouped[row.ts_short][`${m}_raw`] = 0);
        }
        grouped[row.ts_short].count += 1;
        grouped[row.ts_short].experimental_health += row.experimental_health;
        grouped[row.ts_short].penalty_active = grouped[row.ts_short].penalty_active || row.penalty_active;
        activeModules.forEach(m => grouped[row.ts_short][`${m}_raw`] += row[`${m}_raw`]);
      });

      return Object.keys(grouped).map(ts => {
        const g = grouped[ts];
        const avgRow: any = { 
          ts_short: ts, 
          experimental_health: parseFloat((g.experimental_health / g.count).toFixed(2)), 
          penalty_active: g.penalty_active 
        };
        activeModules.forEach(m => avgRow[`${m}_raw`] = parseFloat((g[`${m}_raw`] / g.count).toFixed(2)));
        return avgRow;
      }).sort((a,b) => a.ts_short.localeCompare(b.ts_short));
    }

    return processedRows;
  }, [history, config, activeModules, expWeights, filterSim]);

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
    
    // Explicitly place SOURCE_ID at the front if it exists
    const keys = Object.keys(history[0]).filter(k => k !== 'top_5_features' && k !== 'source_id');
    const cols: ColDef[] = [];
    
    if (history[0].source_id) {
      cols.push({ field: 'source_id', headerName: 'VEHICLE ID', sortable: true, filter: true, width: 130, pinned: 'left' });
    }
    
    keys.forEach(key => {
      cols.push({
        field: key,
        headerName: key.toUpperCase().replace(/_/g, ' '),
        sortable: true,
        filter: true,
        width: key.includes('ts') ? 200 : 140,
        cellStyle: key === 'vehicle_health_score' ? (params: any) => ({
          fontWeight: 'bold', 
          color: params.value < 50 ? '#d32f2f' : (params.value < 80 ? '#f57c00' : '#388e3c')
        }) : undefined
      });
    });
    return cols;
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
          <ToggleButtonGroup value={activeTab} exclusive onChange={(e, val) => val && setActiveTab(val)} size="small" sx={{ bgcolor: 'white' }}>
            <ToggleButton value="operations" sx={{ fontWeight: 'bold', px: 3, borderRadius: 0 }}>LIVE OPERATIONS</ToggleButton>
            <ToggleButton value="experiment" sx={{ fontWeight: 'bold', px: 3, borderRadius: 0 }}>WEIGHT EXPERIMENTATION LAB</ToggleButton>
          </ToggleButtonGroup>
        </Box>
      </Box>

      {/* GLOBAL FILTER CONTEXT */}
      <Paper sx={{ p: 1, borderRadius: 0, display: 'flex', alignItems: 'center', gap: 2 }}>
        <Typography variant="body2" sx={{ fontWeight: 'bold', ml: 1 }}>FILTER CONTEXT:</Typography>
        
        <FormControl size="small" sx={{ minWidth: 200 }}>
          <InputLabel>Active Vehicle (Sim)</InputLabel>
          <Select value={filterSim} onChange={(e) => setFilterSim(e.target.value)} label="Active Vehicle (Sim)" sx={{ borderRadius: 0 }}>
            <MenuItem value="ALL" sx={{ fontWeight: 'bold' }}>ALL VEHICLES (FLEET)</MenuItem>
            {availableSims.map((sim: string) => <MenuItem key={sim} value={sim}>{sim}</MenuItem>)}
          </Select>
        </FormControl>

        <FormControl size="small" sx={{ minWidth: 200 }}>
          <InputLabel>Target Subsystem Lag</InputLabel>
          <Select value={filterModule} onChange={(e) => setFilterModule(e.target.value)} label="Target Subsystem Lag" sx={{ borderRadius: 0 }}>
            <MenuItem value="ALL" sx={{ fontWeight: 'bold' }}>MAX GLOBAL LAG</MenuItem>
            {ALL_MODULES.map(mod => <MenuItem key={mod} value={mod.toUpperCase()}>{mod.toUpperCase()}</MenuItem>)}
          </Select>
        </FormControl>
      </Paper>

      {/* TAB 1: LIVE OPERATIONS */}
      {activeTab === 'operations' && (
        <Box sx={{ display: 'flex', flexDirection: 'column', height: '100%', gap: 2, minHeight: 0 }}>
          
          <Box sx={{ display: 'flex', gap: 2, width: '100%' }}>
            {[
              { label: 'ACTIVE SIMULATIONS', value: availableSims.length },
              { label: 'TOTAL GOLD ROWS', value: metrics?.total_gold_rows?.toLocaleString() || 0 },
              { label: filterModule === 'ALL' ? 'GLOBAL MAX LAG' : `${filterModule} LAG`, value: displayLag.toLocaleString(), color: displayLag > 1000 ? '#d32f2f' : '#212121' },
              { label: filterSim === 'ALL' ? 'LATEST FLEET ANOMALY SCORE' : 'CURRENT VEHICLE HEALTH', value: `${latestRow?.vehicle_health_score || 0}%`, color: (latestRow?.vehicle_health_score || 100) < 60 ? '#d32f2f' : '#2e7d32' }
            ].map((kpi, idx) => (
              <Paper key={idx} sx={{ flex: 1, p: 2, borderRadius: 0, borderLeft: '4px solid #fbc02d' }}>
                <Typography variant="caption" sx={{ color: '#757575', fontWeight: 'bold' }}>{kpi.label}</Typography>
                <Typography variant="h5" sx={{ fontWeight: 'bold', color: kpi.color || '#212121', mt: 0.5 }}>{kpi.value}</Typography>
              </Paper>
            ))}
          </Box>

          <Box sx={{ display: 'flex', gap: 2, flex: 1, minHeight: 0 }}>
            <Paper sx={{ width: '300px', p: 2, borderRadius: 0, display: 'flex', flexDirection: 'column', overflowY: 'auto' }}>
              <Typography variant="caption" sx={{ fontWeight: 'bold', color: '#616161', mb: 2 }}>{filterSim === 'ALL' ? 'LATEST FLEET ANOMALY DRIVERS' : 'LATEST ANOMALY DRIVERS'}</Typography>
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
        <Box sx={{ display: 'flex', gap: 2, flex: 1, minHeight: 0 }}>
          
          <Paper sx={{ width: '320px', p: 2, borderRadius: 0, display: 'flex', flexDirection: 'column', overflowY: 'auto', bgcolor: 'white' }}>
            <Typography variant="subtitle2" sx={{ fontWeight: 800, color: '#212121', mb: 2 }}>DYNAMIC WEIGHT CONFIG</Typography>
            
            {isUnbalanced && (
              <Alert 
                severity="warning" icon={false} sx={{ borderRadius: 0, mb: 2, '& .MuiAlert-message': { p: 0 } }}
                action={<Button size="small" color="inherit" onClick={autoBalanceWeights} startIcon={<AutoFixHighIcon />}>RECALIBRATE</Button>}
              >
                <Typography variant="caption" sx={{ fontWeight: 'bold', display: 'block' }}>Sum is {currentWeightSum.toFixed(3)} (Target: 1.0)</Typography>
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
                      size="small" value={expWeights[mod] || 0} onChange={(_, val) => handleWeightChange(mod, val as number)}
                      min={0} max={1} step={0.01} disabled={!isActive}
                      sx={{ color: COLORS[mod as keyof typeof COLORS], ml: 4, width: 'calc(100% - 32px)', mt: -1 }}
                    />
                  </Box>
                );
              })}
            </Box>
          </Paper>

          <Paper sx={{ flex: 1, p: 2, borderRadius: 0, display: 'flex', flexDirection: 'column' }}>
            <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 2 }}>
              <Typography variant="subtitle2" sx={{ fontWeight: 800, color: '#212121' }}>
                {filterSim === 'ALL' ? 'FLEET-WIDE AVERAGED HEALTH TRAJECTORY' : `RECALCULATED HEALTH TRAJECTORY: ${filterSim}`}
              </Typography>
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
                
                {activeModules.map(mod => (
                  <Line key={`${mod}_raw`} type="monotone" dataKey={`${mod}_raw`} name={`${mod.toUpperCase()} (Raw)`} stroke={COLORS[mod as keyof typeof COLORS]} strokeWidth={1} strokeDasharray="5 5" dot={false} />
                ))}

                <Line type="monotone" dataKey="experimental_health" name="FUSED VEHICLE HEALTH" stroke="#1976d2" strokeWidth={4} dot={false} activeDot={{ r: 8 }} />
              </LineChart>
            </ResponsiveContainer>
          </Paper>
        </Box>
      )}
    </Box>
  );
}