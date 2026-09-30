"""定义基础服务允许登记的资料类别。"""

ALLOWED_CATEGORIES = frozenset({
    "partner_profile",
    "jurisdiction_registry",
    "cooperation_resource",
    "operator_assignment",
})


def is_allowed_category(value: str) -> bool:
    return value in ALLOWED_CATEGORIES
