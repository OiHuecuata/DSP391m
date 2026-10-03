export function ComingSoon({ title, description }: { title: string; description: string }) {
  return <div className="empty-state card">
    <span className="empty-icon" aria-hidden="true">◇</span>
    <span className="eyebrow">Coming soon</span>
    <h2>{title}</h2><p>{description}</p>
  </div>;
}
