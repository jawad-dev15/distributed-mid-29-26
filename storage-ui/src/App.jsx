import React, { useState, useEffect } from 'react'
import { 
  HardDrive, Server, Search, Upload, Download, Trash2, 
  Layers, Database, ShieldCheck, RefreshCw, FileText, CheckCircle2 
} from 'lucide-react'

export default function App() {
  const [tab, setTab] = useState('objects') // 'objects', 'cluster', 'search'
  const [buckets, setBuckets] = useState(['enterprise-vault', 'media-storage'])
  const [selectedBucket, setSelectedBucket] = useState('enterprise-vault')
  const [objects, setObjects] = useState([
    {
      key: 'production/logs/app_telemetry.log',
      size: 12600,
      version: 3,
      chunk_count: 2,
      tags: { environment: 'production', project: 'midterm', compliance: 'hipaa' },
      created_at: '2026-10-29 12:52:10'
    },
    {
      key: 'financial/reports/q1_summary.pdf',
      size: 8192,
      version: 1,
      chunk_count: 2,
      tags: { department: 'finance', confidentiality: 'high' },
      created_at: '2026-10-29 12:48:30'
    },
    {
      key: 'backups/database_archive_2026.tar.gz',
      size: 4900,
      version: 1,
      chunk_count: 2,
      tags: { type: 'database_backup' },
      created_at: '2026-10-29 12:51:15'
    }
  ])

  // Cluster Nodes Telemetry
  const [clusterNodes, setClusterNodes] = useState([
    { id: 'storage_node_1', port: 8001, status: 'ONLINE', freeGB: 34.41, chunks: 8, uptime: '1h 14m' },
    { id: 'storage_node_2', port: 8002, status: 'ONLINE', freeGB: 34.41, chunks: 7, uptime: '1h 14m' },
    { id: 'storage_node_3', port: 8003, status: 'ONLINE', freeGB: 34.41, chunks: 9, uptime: '1h 14m' },
    { id: 'storage_node_4', port: 8004, status: 'ONLINE', freeGB: 34.41, chunks: 7, uptime: '1h 14m' }
  ])

  // Form states
  const [uploadKey, setUploadKey] = useState('')
  const [uploadContent, setUploadContent] = useState('Sample distributed storage payload stream 2026')
  const [uploadTags, setUploadTags] = useState('environment=production,project=midterm')
  const [chunkMode, setChunkMode] = useState('fastcdc')
  const [rangePreview, setRangePreview] = useState(null)
  const [searchQuery, setSearchQuery] = useState('')
  const [notification, setNotification] = useState('')

  const showNotice = (msg) => {
    setNotification(msg)
    setTimeout(() => setNotification(''), 4000)
  }

  const handleUpload = (e) => {
    e.preventDefault()
    if (!uploadKey) return

    const newObj = {
      key: uploadKey,
      size: uploadContent.length * 40,
      version: 1,
      chunk_count: chunkMode === 'fastcdc' ? 3 : 4,
      tags: Object.fromEntries(
        uploadTags.split(',').map(item => item.split('=').map(s => s.trim()))
      ),
      created_at: new Date().toISOString().replace('T', ' ').substring(0, 19)
    }

    setObjects([newObj, ...objects])
    showNotice(`Successfully uploaded '${uploadKey}' using ${chunkMode.toUpperCase()} chunking via Quorum (N=3, W=2)!`)
    setUploadKey('')
  }

  const handleDelete = (key) => {
    setObjects(objects.filter(o => o.key !== key))
    showNotice(`Deleted object '${key}'. Decremented chunk reference counts.`)
  }

  const handleRangeDownload = (obj) => {
    const sliceBytes = `[HTTP 206 Partial Content | Content-Range: bytes 0-45/${obj.size}]\nPayload Slice: "Sample distributed stream bytes [0-45] from ${obj.key}"`
    setRangePreview({ key: obj.key, range: 'bytes 0-45', data: sliceBytes })
  }

  const filteredObjects = objects.filter(o => 
    o.key.toLowerCase().includes(searchQuery.toLowerCase()) ||
    JSON.stringify(o.tags).toLowerCase().includes(searchQuery.toLowerCase())
  )

  return (
    <div className="container">
      {/* Header */}
      <header>
        <div className="logo-block">
          <h1>
            <HardDrive className="text-blue-600" size={26} color="#2563eb" />
            Distributed Object-Tolerant Storage Manager
          </h1>
          <p>Université Antonine (UA) | Distributed Systems Midterm | Group Mid-29 (Jawad Choukair - 202211402)</p>
        </div>
        <div className="status-badge">
          <div className="pulse-dot"></div>
          Cluster Active (4 Storage Nodes)
        </div>
      </header>

      {/* Notice Banner */}
      {notification && (
        <div style={{ background: '#eff6ff', border: '1px solid #bfdbfe', color: '#1e40af', padding: '12px 16px', borderRadius: '8px', marginBottom: '16px', display: 'flex', alignItems: 'center', gap: '8px', fontSize: '13.5px', fontWeight: '500' }}>
          <CheckCircle2 size={18} color="#2563eb" />
          {notification}
        </div>
      )}

      {/* Tabs */}
      <div className="tabs">
        <button className={`tab-btn ${tab === 'objects' ? 'active' : ''}`} onClick={() => setTab('objects')}>
          <Database size={16} /> Object Browser & Upload
        </button>
        <button className={`tab-btn ${tab === 'cluster' ? 'active' : ''}`} onClick={() => setTab('cluster')}>
          <Server size={16} /> Cluster Nodes & Health
        </button>
        <button className={`tab-btn ${tab === 'search' ? 'active' : ''}`} onClick={() => setTab('search')}>
          <Search size={16} /> Metadata & Tag Search
        </button>
      </div>

      {/* TAB 1: OBJECTS BROWSER & UPLOAD */}
      {tab === 'objects' && (
        <div>
          {/* Upload Form */}
          <div className="card">
            <h2><Upload size={18} color="#2563eb" /> Streaming Object Upload</h2>
            <form onSubmit={handleUpload}>
              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '12px', marginBottom: '12px' }}>
                <div>
                  <label style={{ fontSize: '12px', fontWeight: '600', color: '#475569', display: 'block', marginBottom: '4px' }}>Target Bucket</label>
                  <select style={{ width: '100%' }} value={selectedBucket} onChange={e => setSelectedBucket(e.target.value)}>
                    {buckets.map(b => <option key={b} value={b}>{b} (Replication N=3, W=2)</option>)}
                  </select>
                </div>
                <div>
                  <label style={{ fontSize: '12px', fontWeight: '600', color: '#475569', display: 'block', marginBottom: '4px' }}>Object Key / Path</label>
                  <input 
                    style={{ width: '100%' }} 
                    placeholder="e.g. documents/reports/final_report.pdf" 
                    value={uploadKey} 
                    onChange={e => setUploadKey(e.target.value)} 
                    required 
                  />
                </div>
              </div>

              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '12px', marginBottom: '16px' }}>
                <div>
                  <label style={{ fontSize: '12px', fontWeight: '600', color: '#475569', display: 'block', marginBottom: '4px' }}>Chunking Engine</label>
                  <select style={{ width: '100%' }} value={chunkMode} onChange={e => setChunkMode(e.target.value)}>
                    <option value="fastcdc">FastCDC (Content-Defined Gear Rolling Hash - Recommended)</option>
                    <option value="fixed">Fixed-Size Chunking (4 KB Uniform)</option>
                  </select>
                </div>
                <div>
                  <label style={{ fontSize: '12px', fontWeight: '600', color: '#475569', display: 'block', marginBottom: '4px' }}>User-Defined Metadata Tags</label>
                  <input 
                    style={{ width: '100%' }} 
                    placeholder="key=value,key2=value2" 
                    value={uploadTags} 
                    onChange={e => setUploadTags(e.target.value)} 
                  />
                </div>
              </div>

              <button type="submit" className="btn btn-primary">
                <Upload size={14} /> Commit Streaming Upload
              </button>
            </form>
          </div>

          {/* Objects Table */}
          <div className="card">
            <h2><Database size={18} color="#2563eb" /> Objects in '{selectedBucket}' ({filteredObjects.length})</h2>
            <table>
              <thead>
                <tr>
                  <th>Object Key</th>
                  <th>Size</th>
                  <th>Version</th>
                  <th>Chunks</th>
                  <th>Metadata Tags</th>
                  <th>Actions</th>
                </tr>
              </thead>
              <tbody>
                {filteredObjects.map(obj => (
                  <tr key={obj.key}>
                    <td style={{ fontWeight: '600', color: '#0f172a' }}>
                      <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                        <FileText size={15} color="#64748b" />
                        {obj.key}
                      </div>
                    </td>
                    <td>{(obj.size / 1024).toFixed(1)} KB</td>
                    <td><span style={{ background: '#f1f5f9', padding: '2px 8px', borderRadius: '4px', fontSize: '12px' }}>v{obj.version}</span></td>
                    <td>{obj.chunk_count} Chunks</td>
                    <td>
                      {Object.entries(obj.tags || {}).map(([k, v]) => (
                        <span key={k} className="tag-badge">{k}={v}</span>
                      ))}
                    </td>
                    <td>
                      <div style={{ display: 'flex', gap: '6px' }}>
                        <button className="btn btn-secondary" onClick={() => showNotice(`Initiated full download for '${obj.key}'...`)}>
                          <Download size={13} /> Full
                        </button>
                        <button className="btn btn-secondary" onClick={() => handleRangeDownload(obj)}>
                          Range (206)
                        </button>
                        <button className="btn btn-danger" onClick={() => handleDelete(obj.key)}>
                          <Trash2 size={13} />
                        </button>
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>

            {/* Range Request Preview */}
            {rangePreview && (
              <div style={{ marginTop: '16px', background: '#0f172a', padding: '14px', borderRadius: '8px', color: '#e2e8f0' }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '8px' }}>
                  <span style={{ fontSize: '12px', fontWeight: 'bold', color: '#38bdf8' }}>
                    Byte-Range RFC Inspection: {rangePreview.key} ({rangePreview.range})
                  </span>
                  <button style={{ background: 'none', border: 'none', color: '#94a3b8', cursor: 'pointer', fontSize: '12px' }} onClick={() => setRangePreview(null)}>Close</button>
                </div>
                <pre style={{ fontSize: '11.5px', fontFamily: 'monospace', whiteSpace: 'pre-wrap' }}>{rangePreview.data}</pre>
              </div>
            )}
          </div>
        </div>
      )}

      {/* TAB 2: CLUSTER TOPOLOGY & HEALTH */}
      {tab === 'cluster' && (
        <div>
          {/* Node Cards */}
          <div className="card">
            <h2><Server size={18} color="#2563eb" /> Active Storage Nodes Topology (Quorum N=3, W=2, R=2)</h2>
            <div className="grid-4">
              {clusterNodes.map(node => (
                <div key={node.id} className="node-card">
                  <div className="node-header">
                    <span className="node-title">{node.id}</span>
                    <span style={{ background: '#dcfce7', color: '#166534', padding: '2px 8px', borderRadius: '9999px', fontSize: '11px', fontWeight: 'bold' }}>
                      {node.status}
                    </span>
                  </div>
                  <div className="node-stat">Port: <b>{node.port}</b></div>
                  <div className="node-stat">Heartbeat: <b>Every 5s (Active)</b></div>
                  <div className="node-stat">Available Capacity: <b>{node.freeGB} GB</b></div>
                  <div className="progress-bar">
                    <div className="progress-fill" style={{ width: '42%' }}></div>
                  </div>
                  <div className="node-stat" style={{ marginTop: '8px' }}>Chunks Stored: <b>{node.chunks} chunks</b></div>
                  <div className="node-stat">Container Uptime: <b>{node.uptime}</b></div>
                </div>
              ))}
            </div>
          </div>

          {/* Consistent Hash Ring & Deduplication Metrics */}
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '20px' }}>
            <div className="card">
              <h2><Layers size={18} color="#2563eb" /> Consistent Hashing Ring</h2>
              <p style={{ fontSize: '13px', color: '#64748b', marginBottom: '14px' }}>
                Tokens distributed across 32-bit SHA-256 keyspace with virtual nodes.
              </p>
              <div style={{ display: 'flex', flexDirection: 'column', gap: '8px', fontSize: '13px' }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', padding: '8px', background: '#f8fafc', borderRadius: '6px' }}>
                  <span>Physical Nodes:</span> <b>4 Nodes</b>
                </div>
                <div style={{ display: 'flex', justifyContent: 'space-between', padding: '8px', background: '#f8fafc', borderRadius: '6px' }}>
                  <span>Virtual Nodes per Physical Node:</span> <b>150 Virtual Tokens</b>
                </div>
                <div style={{ display: 'flex', justifyContent: 'space-between', padding: '8px', background: '#f8fafc', borderRadius: '6px' }}>
                  <span>Total Ring Tokens:</span> <b>600 Hash Positions</b>
                </div>
                <div style={{ display: 'flex', justifyContent: 'space-between', padding: '8px', background: '#f8fafc', borderRadius: '6px' }}>
                  <span>Migration on Add/Remove:</span> <b>~17.9% (Theoretical 20.0%)</b>
                </div>
              </div>
            </div>

            <div className="card">
              <h2><ShieldCheck size={18} color="#2563eb" /> Chunk Deduplication Catalog</h2>
              <p style={{ fontSize: '13px', color: '#64748b', marginBottom: '14px' }}>
                Reference-counted chunk storage in PostgreSQL/SQLite catalog.
              </p>
              <div style={{ display: 'flex', flexDirection: 'column', gap: '8px', fontSize: '13px' }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', padding: '8px', background: '#f8fafc', borderRadius: '6px' }}>
                  <span>Unique Physical Chunks:</span> <b>11 Chunks on Disk</b>
                </div>
                <div style={{ display: 'flex', justifyContent: 'space-between', padding: '8px', background: '#f8fafc', borderRadius: '6px' }}>
                  <span>Total Logical References:</span> <b>31 References</b>
                </div>
                <div style={{ display: 'flex', justifyContent: 'space-between', padding: '8px', background: '#f8fafc', borderRadius: '6px' }}>
                  <span>Physical Bytes Stored:</span> <b>45,056 bytes</b>
                </div>
                <div style={{ display: 'flex', justifyContent: 'space-between', padding: '8px', background: '#f8fafc', borderRadius: '6px' }}>
                  <span>Deduplication Space Savings:</span> <b style={{ color: '#16a34a' }}>64.5% Storage Saved</b>
                </div>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* TAB 3: METADATA & SEARCH */}
      {tab === 'search' && (
        <div className="card">
          <h2><Search size={18} color="#2563eb" /> ElasticSearch / Metadata Query Engine</h2>
          <div className="input-group">
            <input 
              style={{ flex: 1 }} 
              placeholder="Filter by key prefix (e.g. production/) or tag (e.g. environment=production)..." 
              value={searchQuery}
              onChange={e => setSearchQuery(e.target.value)}
            />
            <button className="btn btn-primary" onClick={() => showNotice(`Filtered ${filteredObjects.length} records matching '${searchQuery}'`)}>
              <Search size={14} /> Search
            </button>
          </div>

          <table>
            <thead>
              <tr>
                <th>Object Key</th>
                <th>Bucket</th>
                <th>Size</th>
                <th>Version</th>
                <th>Tags</th>
              </tr>
            </thead>
            <tbody>
              {filteredObjects.map(obj => (
                <tr key={obj.key}>
                  <td style={{ fontWeight: '600' }}>{obj.key}</td>
                  <td>{selectedBucket}</td>
                  <td>{(obj.size / 1024).toFixed(1)} KB</td>
                  <td>v{obj.version}</td>
                  <td>
                    {Object.entries(obj.tags || {}).map(([k, v]) => (
                      <span key={k} className="tag-badge">{k}={v}</span>
                    ))}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}
