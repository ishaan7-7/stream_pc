import React, { useMemo, useState } from 'react';
import { 
  Box, Typography, Paper, Chip, ToggleButton, ToggleButtonGroup, 
  Divider, Button, CircularProgress, Alert, AlertTitle 
} from '@mui/material';
import ArrowBackIcon from '@mui/icons-material/ArrowBack';
import PsychologyIcon from '@mui/icons-material/Psychology';
import { AgGridReact } from 'ag-grid-react';
import type { ColDef } from 'ag-grid-community';
import { ModuleRegistry, AllCommunityModule } from 'ag-grid-community';
import 'ag-grid-community/styles/ag-grid.css';
import 'ag-grid-community/styles/ag-theme-balham.css';
import { useQuery } from '@tanstack/react-query';
import axios from 'axios';
import { useStore } from '../store';
import Plot from 'react-plotly.js';

ModuleRegistry.registerModules([AllCommunityModule]);

const fetchAlertsMetrics = async () => (await axios.get('http://127.0.0.1:8005/api/alerts/metrics')).data;
const fetchDtcAnalysis = async (module: string, source_id: string, peak_ts: string) => {
  // Encode the timestamp to safely handle spaces and plus signs in the URL
  const encodedTs = encodeURIComponent(peak_ts);
  const { data } = await axios.get(`http://127.0.0.1:8005/api/dtc/analyze?module=${module}&source_id=${source_id}&peak_ts=${encodedTs}`);
  return data;
};

export default function DashboardAlerts() {
  const { autoRefresh } = useStore();
  
  const [activeTab, setActiveTab] = useState<'OPEN' | 'CLOSED'>('OPEN');
  
  // Deep Dive State
  const [selectedAlert, setSelectedAlert] = useState<any | null>(null);
  const [dtcData, setDtcData] = useState<any | null>(null);
  const [isAnalyzing, setIsAnalyzing] = useState(false);
  const [analysisError, setAnalysisError] = useState<string | null>(null);

  // Queries
  const { data: metrics, isLoading: metricsLoading } = useQuery({ 
    queryKey: ['alertsMetrics'], 
    queryFn: fetchAlertsMetrics, 
    // Suspend auto-refresh if the user is deep-diving so the UI doesn't jump
    refetchInterval: (autoRefresh && !selectedAlert) ? 2000 : false, 
  });

  const handleDeepDive = async (alertRow: any) => {
    setSelectedAlert(alertRow);
    setIsAnalyzing(true);
    setAnalysisError(null);
    setDtcData(null);
    try {
      // Note: mapping 'alerts_start_ts' to 'alert_start_ts' depending on python output typo
      const ts = alertRow.peak_anomaly_ts; 
      const res = await fetchDtcAnalysis(alertRow.module, alertRow.source_id, ts);
      if (res.error) {
        setAnalysisError(res.error);
      } else {
        setDtcData(res);
      }
    } catch (err: any) {
      setAnalysisError(err.message || "Failed to contact DTC backend.");
    } finally {
      setIsAnalyzing(false);
    }
  };

  const closeDeepDive = () => {
    setSelectedAlert(null);
    setDtcData(null);
    setAnalysisError(null);
  };

  // --- GRID LOGIC ---
  const tableColDefs = useMemo<ColDef[]>(() => [
    { 
      field: 'alert_id', 
      headerName: 'ALERT ID', 
      flex: 1,
      minWidth: 120, 
      valueFormatter: p => p.value ? p.value.substring(0, 8) : '',
      cellStyle: { fontFamily: 'monospace', fontWeight: 'bold' }
    },
    { 
      field: 'module', 
      headerName: 'MODULE', 
      flex: 1, 
      minWidth: 130, 
      valueFormatter: p => p.value?.toUpperCase() 
    },
    { 
      field: 'source_id', 
      headerName: 'VEHICLE', 
      flex: 1, 
      minWidth: 120 
    },
    { 
      field: 'peak_anomaly_ts', 
      headerName: 'PEAK ANOMALY TS', 
      flex: 1.5, // Gives the timestamp column slightly more room to breathe
      minWidth: 200 
    },
    { 
      field: 'max_composite_score', 
      headerName: 'SEVERITY SCORE', 
      flex: 1,
      minWidth: 140, 
      type: 'numericColumn',
      valueFormatter: p => p.value ? parseFloat(p.value).toFixed(2) : ''
    },
    {
      headerName: 'ACTION',
      width: 160, // Kept as fixed width so the button stays a consistent size
      pinned: 'right',
      cellRenderer: (params: any) => (
        <Button 
          size="small" 
          variant="contained" 
          color="error" 
          startIcon={<PsychologyIcon />}
          onClick={() => handleDeepDive(params.data)}
          sx={{ height: '24px', fontSize: '10px', mt: 0.5, borderRadius: 0, boxShadow: 'none' }}
        >
          ROOT CAUSE
        </Button>
      )
    }
  ], []);

  return (
    <Box sx={{ height: 'calc(100vh - 80px)', display: 'flex', flexDirection: 'column', gap: 2, p: 2, bgcolor: '#f5f5f5' }}>
      
      {/* HEADER */}
      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-end', borderBottom: '2px solid #bdbdbd', pb: 1 }}>
        <Typography variant="h5" sx={{ fontWeight: 700, color: '#212121', letterSpacing: '-0.5px' }}>
          OPERATIONS CENTER: FLEET ALERTS
        </Typography>
        
        {!selectedAlert && (
          <ToggleButtonGroup value={activeTab} exclusive onChange={(e, val) => val && setActiveTab(val)} size="small" sx={{ bgcolor: 'white' }}>
            <ToggleButton value="OPEN" sx={{ fontWeight: 'bold', px: 3, borderRadius: 0, color: '#d32f2f', '&.Mui-selected': { bgcolor: '#ffebee', color: '#d32f2f' } }}>🔴 ACTIVE (OPEN)</ToggleButton>
            <ToggleButton value="CLOSED" sx={{ fontWeight: 'bold', px: 3, borderRadius: 0 }}>📜 RESOLVED (CLOSED)</ToggleButton>
          </ToggleButtonGroup>
        )}
      </Box>

      {/* KPI CARDS */}
      {!selectedAlert && (
        <Box sx={{ display: 'flex', gap: 2, width: '100%' }}>
          {[
            { label: '🔴 ACTIVE ALERTS', value: metrics?.active_alerts_count?.toLocaleString() || 0, color: '#d32f2f' },
            { label: '⚠️ CRITICAL VEHICLES', value: metrics?.critical_vehicles?.toLocaleString() || 0, color: '#f57c00' },
            { label: 'PROCESSING LAG', value: metrics?.processing_lag?.toLocaleString() || 0 }
          ].map((kpi, idx) => (
            <Paper key={idx} sx={{ flex: 1, p: 2, borderRadius: 0, borderLeft: `4px solid ${kpi.color || '#424242'}` }}>
              <Typography variant="caption" sx={{ color: '#757575', fontWeight: 'bold' }}>{kpi.label}</Typography>
              <Typography variant="h5" sx={{ fontWeight: 'bold', color: kpi.color || '#212121', mt: 0.5 }}>{kpi.value}</Typography>
            </Paper>
          ))}
        </Box>
      )}

      {/* MASTER VIEW: DATA GRID */}
      {!selectedAlert && (
        <Paper sx={{ display: 'flex', flexDirection: 'column', p: 0, borderRadius: 0, flex: 1, minHeight: 0 }}>
          <Box className="ag-theme-balham" sx={{ flexGrow: 1, width: '100%' }}>
            <AgGridReact
              rowData={tableData}
              columnDefs={tableColDefs}
              animateRows={false}
              defaultColDef={{ resizable: true, sortable: true, filter: true }}
              overlayLoadingTemplate={metricsLoading ? '<span class="ag-overlay-loading-center">Fetching Alerts...</span>' : undefined}
              overlayNoRowsTemplate='<span class="ag-overlay-loading-center">No Alerts Found</span>'
            />
          </Box>
        </Paper>
      )}

      {/* DETAIL VIEW: DTC DEEP DIVE */}
      {selectedAlert && (
        <Paper sx={{ flex: 1, display: 'flex', flexDirection: 'column', p: 2, borderRadius: 0, overflowY: 'auto' }}>
          
          {/* DEEP DIVE HEADER */}
          <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
            <Box>
              <Button startIcon={<ArrowBackIcon />} onClick={closeDeepDive} sx={{ mb: 1, fontWeight: 'bold', color: '#616161' }}>
                BACK TO ALERTS
              </Button>
              <Typography variant="h6" sx={{ fontWeight: 800 }}>
                🧠 NEURAL DEEP DIVE: {selectedAlert.source_id} ({selectedAlert.module?.toUpperCase()})
              </Typography>
              <Typography variant="caption" sx={{ color: '#757575' }}>
                Peak Anomaly: {selectedAlert.peak_anomaly_ts} | Alert ID: {selectedAlert.alert_id}
              </Typography>
            </Box>
            <Chip label={selectedAlert.status} color={selectedAlert.status === 'OPEN' ? 'error' : 'default'} sx={{ borderRadius: 0, fontWeight: 'bold' }} />
          </Box>
          <Divider sx={{ mb: 3 }} />

          {/* LOADING STATE */}
          {isAnalyzing && (
            <Box sx={{ display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', flex: 1, gap: 2 }}>
              <CircularProgress size={50} />
              <Typography variant="body1" sx={{ fontWeight: 'bold', color: '#616161' }}>
                Running PyTorch Inference and Mathematical Integration...
              </Typography>
            </Box>
          )}

          {/* ERROR STATE */}
          {!isAnalyzing && analysisError && (
            <Alert severity="error" sx={{ borderRadius: 0 }}>
              <AlertTitle sx={{ fontWeight: 'bold' }}>Analysis Failed</AlertTitle>
              {analysisError}
            </Alert>
          )}

          {/* RESULTS STATE */}
          {!isAnalyzing && dtcData && !analysisError && (
            <Box sx={{ display: 'flex', flexDirection: 'column', gap: 3 }}>
              
              {/* DIAGNOSTIC MESSAGES */}
              <Box sx={{ display: 'flex', gap: 2, flexDirection: 'column' }}>
                {dtcData.diagnostics?.models_loaded === 0 && (
                  <Alert severity="error" sx={{ borderRadius: 0 }}>
                    <strong>CRITICAL ERROR:</strong> No trained PyTorch models found in memory for the '{selectedAlert.module}' module. Check your DTC artifacts folder.
                  </Alert>
                )}

                {dtcData.diagnostics?.skipped_dtcs && Object.keys(dtcData.diagnostics.skipped_dtcs).length > 0 && (
                  <Alert severity="warning" sx={{ borderRadius: 0 }}>
                    <AlertTitle sx={{ fontWeight: 'bold' }}>Schema Mismatch</AlertTitle>
                    Some DTC codes were skipped because their required features are missing in the Silver table.
                    <ul>
                      {Object.entries(dtcData.diagnostics.skipped_dtcs).map(([code, missing]: any) => (
                        <li key={code}><strong>{code} Ignored:</strong> Missing columns -> {missing}</li>
                      ))}
                    </ul>
                  </Alert>
                )}

                {/* TRIGGERS */}
                <Box>
                  <Typography variant="subtitle2" sx={{ fontWeight: 'bold', mb: 1 }}>DIAGNOSTIC OUTPUT</Typography>
                  {dtcData.triggers && dtcData.triggers.length > 0 ? (
                    dtcData.triggers.map((t: any, i: number) => (
                      <Alert key={i} severity={t.severity === 'CRITICAL' ? 'error' : 'warning'} sx={{ borderRadius: 0, mb: 1, py: 0 }}>
                        <strong>{t.code} ({t.severity}):</strong> {t.message}
                      </Alert>
                    ))
                  ) : (
                    <Alert severity="success" sx={{ borderRadius: 0 }}>
                      ✅ Neural Network found no specific known DTC signatures. (Likely general physical wear).
                    </Alert>
                  )}
                </Box>
              </Box>

              {/* PLOTLY CHARTS */}
              <Box sx={{ display: 'flex', gap: 2, height: '400px' }}>
                <Paper sx={{ flex: 1, borderRadius: 0, border: '1px solid #e0e0e0', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
                  {dtcData.critical_plot ? (
                    <Plot
                      data={dtcData.critical_plot.data}
                      layout={{ ...dtcData.critical_plot.layout, autosize: true }}
                      useResizeHandler={true}
                      style={{ width: '100%', height: '100%' }}
                      config={{ displayModeBar: false }}
                    />
                  ) : (
                    <Typography variant="caption" color="textSecondary">No Critical DTCs monitored for this module.</Typography>
                  )}
                </Paper>
                
                <Paper sx={{ flex: 1, borderRadius: 0, border: '1px solid #e0e0e0', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
                  {dtcData.non_critical_plot ? (
                    <Plot
                      data={dtcData.non_critical_plot.data}
                      layout={{ ...dtcData.non_critical_plot.layout, autosize: true }}
                      useResizeHandler={true}
                      style={{ width: '100%', height: '100%' }}
                      config={{ displayModeBar: false }}
                    />
                  ) : (
                    <Typography variant="caption" color="textSecondary">No Non-Critical DTCs monitored for this module.</Typography>
                  )}
                </Paper>
              </Box>
            </Box>
          )}
        </Paper>
      )}
    </Box>
  );
}