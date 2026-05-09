import re
from typing import Any, Iterable, Optional
from urllib.parse import parse_qs, quote, unquote, urlsplit, urlunsplit


def is_worten_product_url(url: str, require_host: bool = False) -> bool:
    try:
        parts = urlsplit(str(url).strip())
        host = (parts.hostname or "").lower()
        if require_host and host and host != "worten.pt" and not host.endswith(".worten.pt"):
            return False
        segments = [segment for segment in parts.path.split("/") if segment]
        if len(segments) < 2:
            return False
        route = unquote(segments[0])
        return route == "produtos" and bool(segments[1])
    except Exception:
        return False


def is_allowed_worten_product_url(url: str) -> bool:
    try:
        parts = urlsplit(str(url).strip())
        host = (parts.hostname or "").lower()
        return parts.scheme == "https" and (host == "worten.pt" or host.endswith(".worten.pt")) and is_worten_product_url(url)
    except Exception:
        return False


def extract_seller_id(url: str) -> Optional[str]:
    try:
        params = parse_qs(urlsplit(str(url)).query)
        for seller_id in params.get("seller_id", []):
            seller_id = seller_id.strip()
            if seller_id:
                return seller_id
        for facet_filter in params.get("facetFilters", []):
            marker = "seller_id:"
            if marker in facet_filter:
                seller_id = facet_filter.split(marker, 1)[1].strip().strip('[]"\'')
                for sep in (",", ";", "|"):
                    seller_id = seller_id.split(sep, 1)[0]
                if seller_id:
                    return seller_id
    except Exception:
        return None
    return None


def parse_pages_to_scrape(pages_value: Any, default_count: int) -> Iterable[int]:
    pages_str = str(pages_value) if pages_value else ""
    if pages_str and pages_str.lower() != "nan":
        pages = []
        for token in pages_str.replace("，", ",").split(","):
            token = token.strip()
            if not token:
                continue
            try:
                page_num = float(token)
            except Exception:
                continue
            if page_num.is_integer() and page_num > 0:
                pages.append(int(page_num))
        if pages:
            return pages
    return range(1, default_count + 1)


def append_page_param(url: str, page_num: int) -> str:
    parsed = urlsplit(url)
    query_parts = [q for q in parsed.query.split("&") if q and not q.startswith("page=")]
    query_parts.append(f"page={page_num}")
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, "&".join(query_parts), parsed.fragment))


def seller_search_url(seller_id: str) -> str:
    return f"https://www.worten.pt/search?query=*&facetFilters=seller_id:{quote(seller_id, safe='')}"
