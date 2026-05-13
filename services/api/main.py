from fastapi import FastAPI, HTTPException, Query

from cassandra_client import (
    get_domain_pages,
    get_domains,
    get_editor_patterns,
    get_hourly_report,
    get_page,
    get_user_pages,
)

app = FastAPI(title="Wikipedia Analytics Platform", version="2.0.0")


@app.get("/health")
def health():
    return {"status": "ok", "stack": ["Kafka", "Spark", "Cassandra", "MinIO", "FastAPI"]}


@app.get("/api/domains")
def domains():
    return get_domains()


@app.get("/api/users/{user_id}/pages")
def pages_by_user(user_id: int, limit: int = Query(100, ge=1, le=1000)):
    return get_user_pages(user_id, limit)


@app.get("/api/pages/{page_id}")
def page_details(page_id: int):
    page = get_page(page_id)
    if page is None:
        raise HTTPException(status_code=404, detail="Page not found")
    return page


@app.get("/api/domains/{domain}/pages")
def pages_by_domain(domain: str, from_: str = Query(..., alias="from"), to: str = Query(...), limit: int = Query(100, ge=1, le=1000)):
    return get_domain_pages(domain, from_, to, limit)


@app.get("/api/reports/hourly")
def hourly_report(domain: str, hours: int = Query(6, ge=1, le=24)):
    return get_hourly_report(domain, hours)


@app.get("/api/analytics/editor-patterns")
def editor_patterns(min_pages: int = Query(5, ge=1)):
    return get_editor_patterns(min_pages)
