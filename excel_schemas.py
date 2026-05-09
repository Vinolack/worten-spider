from typing import Any, Dict, Iterable, List

PRODUCT_COLUMNS = [
    '商品链接', '产品评分', 'EAN', 'SKU', '品牌', '类目',
    '标题', '描述', '销售和发货方', '运费', '当前售价（最低）',
    '图1', '图2', '图3', '图4', '图5',
    '店铺1', '售价1', '运费1', '店铺2', '售价2', '运费2', '店铺3', '售价3', '运费3',
]

SELLER_COLUMNS = ['初始链接', '店铺名称', '链接', '店铺运费', '送货时间']
PRICE_COLUMNS = ['商品链接', '价格', '运费', '销售和发货方']

PRODUCT_SHEET = '商品链接数据 (Product Links)'
SHOP_SHEET = '店铺链接数据 (Shop Products)'
SELLER_SHEET = '跟卖链接数据 (Sellers)'
PRICE_SHEET = '商品价格数据 (Product Prices)'


def filter_row(row: Dict[str, Any], columns: Iterable[str]) -> Dict[str, Any]:
    return {column: row.get(column, '') for column in columns}


def product_failure_row(url: str, reason: str) -> Dict[str, Any]:
    return filter_row({
        '商品链接': url,
        '标题': reason,
        '描述': reason,
        '销售和发货方': reason,
        '运费': reason,
        '当前售价（最低）': reason,
    }, PRODUCT_COLUMNS)


def seller_failure_row(url: str, reason: str) -> Dict[str, Any]:
    return filter_row({
        '初始链接': url,
        '店铺名称': reason,
        '链接': reason,
        '店铺运费': reason,
        '送货时间': reason,
    }, SELLER_COLUMNS)


def price_failure_row(url: str, reason: str) -> Dict[str, Any]:
    return filter_row({
        '商品链接': url,
        '价格': reason,
        '运费': reason,
        '销售和发货方': reason,
    }, PRICE_COLUMNS)


def rows_for_export(rows: List[Dict[str, Any]], columns: List[str]) -> List[Dict[str, Any]]:
    return [filter_row(row, columns) for row in rows]
