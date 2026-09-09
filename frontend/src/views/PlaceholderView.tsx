export function PlaceholderView({ name }: { name: string }) {
  return (
    <section className="rounded-card border border-border bg-surface p-6">
      <h1 className="text-lg font-semibold">{name}</h1>
      <p className="mt-2 text-muted">This view is rebuilt in the next pull request.</p>
    </section>
  );
}
