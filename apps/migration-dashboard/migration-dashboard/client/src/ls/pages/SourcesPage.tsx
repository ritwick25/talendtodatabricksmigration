import { useEffect, useRef, useState } from 'react';
import { useAnalyticsQuery } from '@databricks/appkit-ui/react';
import { Icon } from '../icons';
import { ComplexityBar, Empty } from '../components';
import { useShell } from '../ShellState';
import { updateControl, uploadSourceFile, runAppJob } from '../../lib/api';

export function SourcesPage() {
  const { data: control } = useAnalyticsQuery('control_status', {});
  const { data: queueData } = useAnalyticsQuery('queue_list', {});
  const { toast } = useShell();
  const c = control?.[0];
  const rows = queueData ?? [];
  const pending = rows.filter((r) => r.status === 'PENDING');

  const [sourcePath, setSourcePath] = useState('');
  const [destPath, setDestPath] = useState('');
  const [savingPaths, setSavingPaths] = useState(false);
  const [selected, setSelected] = useState<string[]>([]);
  const [uploading, setUploading] = useState(false);
  const fileInput = useRef<HTMLInputElement>(null);
  const dirInput = useRef<HTMLInputElement>(null);

  useEffect(() => {
    if (c) {
      setSourcePath(c.source_path ?? '');
      setDestPath(c.destination_path ?? '');
    }
  }, [c]);

  const savePaths = async () => {
    setSavingPaths(true);
    try {
      await updateControl({ source_path: sourcePath, destination_path: destPath });
      toast('Source and destination saved.');
    } catch (err) {
      toast(err instanceof Error ? err.message : String(err), 'warn');
    } finally {
      setSavingPaths(false);
    }
  };

  const handleUpload = async (files: FileList | null) => {
    if (!files || !files.length) return;
    setUploading(true);
    try {
      for (const f of Array.from(files)) {
        await uploadSourceFile(f.webkitRelativePath || f.name, f);
      }
      toast(`Uploaded ${files.length} file(s) to the samples volume under raw/.`, undefined);
    } catch (err) {
      toast(err instanceof Error ? err.message : String(err), 'warn');
    } finally {
      setUploading(false);
    }
  };

  const toggle = (id: string, checked: boolean) => {
    setSelected(checked ? [...selected, id] : selected.filter((x) => x !== id));
  };

  const queueAndRun = async () => {
    if (!selected.length) {
      toast('Select at least one job first.', 'warn');
      return;
    }
    try {
      const result = await runAppJob({ pipelineIds: selected.join(','), batchSize: '', maxBatches: '1' });
      toast(`Triggered run ${result.runId} for ${selected.length} job(s).`);
      setSelected([]);
    } catch (err) {
      toast(err instanceof Error ? err.message : String(err), 'warn');
    }
  };

  return (
    <>
      <section className="g g-21">
        <div className="card">
          <div className="hd">
            <h3>Source and destination</h3>
            <span className="eyebrow">where the migration reads from and writes to</span>
          </div>
          <div className="eyebrow" style={{ margin: '0 0 8px' }}>
            Source · Unity Catalog Volume with the Talend export
          </div>
          <div className="row">
            <input
              className="inp mono"
              value={sourcePath}
              onChange={(e) => setSourcePath(e.target.value)}
              placeholder="/Volumes/catalog/schema/talend_export"
              spellCheck={false}
              aria-label="Source volume path"
            />
          </div>
          <div className="eyebrow" style={{ margin: '18px 0 8px' }}>
            Destination · catalog.schema the migrated tables write into
          </div>
          <div className="row">
            <input
              className="inp mono"
              value={destPath}
              onChange={(e) => setDestPath(e.target.value)}
              placeholder="catalog.schema"
              spellCheck={false}
              aria-label="Destination catalog.schema"
            />
          </div>
          <div className="row" style={{ marginTop: 14 }}>
            <button className="btn" disabled={savingPaths} onClick={savePaths}>
              Save source and destination
            </button>
          </div>
          <p className="hint" style={{ marginTop: 8 }}>
            Saved to the control table. The generated code's target tables still need to be parameterized from this value
            -- that wiring isn't built yet.
          </p>
        </div>
        <div className="card">
          <div className="hd">
            <h3>Add raw Talend files</h3>
            <span className="eyebrow">optional</span>
          </div>
          <div className="dz">
            <Icon name="upload" size={28} />
            <h4>Drop or choose files</h4>
            <p>
              Uploads land in the samples volume under <span className="mono">raw/</span>. Turning newly uploaded exports
              into new queue rows isn't wired up yet -- that still needs a discovery step.
            </p>
            <div className="row" style={{ justifyContent: 'center' }}>
              <button className="btn" disabled={uploading} onClick={() => fileInput.current?.click()}>
                <Icon name="rows" size={16} />
                Choose files
              </button>
              <button className="btn" disabled={uploading} onClick={() => dirInput.current?.click()}>
                <Icon name="folder" size={16} />
                Choose folder
              </button>
            </div>
            <input ref={fileInput} type="file" multiple hidden accept=".item,.properties,.xml,.java" onChange={(e) => handleUpload(e.target.files)} />
            <input
              ref={dirInput}
              type="file"
              multiple
              hidden
              // @ts-expect-error -- webkitdirectory isn't in the DOM lib's InputHTMLAttributes
              webkitdirectory="true"
              onChange={(e) => handleUpload(e.target.files)}
            />
          </div>
        </div>
      </section>

      <section className="card">
        <div className="hd">
          <h3>Pending pipelines</h3>
          <span className="eyebrow">{selected.length} of {pending.length} selected</span>
        </div>
        <div className="tw">
          <table>
            <thead>
              <tr>
                <th></th>
                <th>Job</th>
                <th>Source</th>
                <th>Est. complexity</th>
              </tr>
            </thead>
            <tbody>
              {pending.length === 0 && (
                <tr>
                  <td colSpan={4}>
                    <Empty text="Nothing pending. Jobs show up here once they're in the queue as PENDING." />
                  </td>
                </tr>
              )}
              {pending.map((j) => (
                <tr key={j.pipeline_id}>
                  <td>
                    <input
                      type="checkbox"
                      checked={selected.includes(j.pipeline_id)}
                      onChange={(e) => toggle(j.pipeline_id, e.target.checked)}
                      aria-label={`Select ${j.pipeline_id}`}
                    />
                  </td>
                  <td className="nm">{j.pipeline_id}</td>
                  <td>{j.source ?? '-'}</td>
                  <td>
                    <ComplexityBar value={j.complexity_score} />
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <div className="row" style={{ justifyContent: 'space-between', borderTop: '1px solid var(--line)', paddingTop: 14 }}>
          <div className="row">
            <button className="btn sm" onClick={() => setSelected(pending.map((j) => j.pipeline_id))}>
              Select all
            </button>
            <button className="btn sm" onClick={() => setSelected([])}>
              Select none
            </button>
          </div>
          <button className="btn primary" disabled={!selected.length} onClick={queueAndRun}>
            <Icon name="play" size={16} />
            Run {selected.length} selected
          </button>
        </div>
      </section>
    </>
  );
}
