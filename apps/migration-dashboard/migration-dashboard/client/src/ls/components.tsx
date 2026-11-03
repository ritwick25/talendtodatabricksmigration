import type { ReactNode } from 'react';
import { STG, STGL, deriveStage, type QueueRow } from './domain';

export function StatusPill({ status }: { status: string }) {
  const m: Record<string, [string, string]> = {
    COMPLETED: ['ok', 'Completed'],
    FAILED: ['bad', 'Failed'],
    IN_PROGRESS: ['info live', 'In flight'],
    PENDING: ['', 'Queued'],
  };
  const [cls, label] = m[status] ?? ['', status];
  return (
    <span className={`pill ${cls}`}>
      <i />
      {label}
    </span>
  );
}

export function StageDots({ job }: { job: QueueRow }) {
  const cur = deriveStage(job);
  const idx = cur ? STG.indexOf(cur) : -1;
  return (
    <>
      {STG.map((s, i) => {
        let cls = 'q';
        if (job.status === 'COMPLETED') cls = 'done';
        else if (job.status === 'FAILED') cls = i < idx ? 'done' : i === idx ? 'bad' : 'q';
        else if (job.status === 'IN_PROGRESS') cls = i < idx ? 'done' : i === idx ? 'now' : 'q';
        return <i key={s} className={`sd ${cls}`} title={STGL[s]} />;
      })}
    </>
  );
}

export function Tile({
  label,
  value,
  sub,
  subCls,
}: {
  label: string;
  value: ReactNode;
  sub?: ReactNode;
  subCls?: string;
}) {
  return (
    <div className="card kpi">
      <div className="l">{label}</div>
      <div className="v">{value}</div>
      {sub !== undefined && <div className={`d ${subCls ?? ''}`}>{sub}</div>}
    </div>
  );
}

export function Empty({ text }: { text: string }) {
  return <div className="empty">{text}</div>;
}

export function ComplexityBar({ value }: { value: number | null }) {
  const v = value ?? 0;
  const cls = v > 75 ? 'bad' : v > 50 ? 'warn' : 'ok';
  return (
    <span className={`bar ${cls}`}>
      <i style={{ width: `${v}%` }} />
    </span>
  );
}
