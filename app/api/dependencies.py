from __future__ import annotations

from fastapi import Header

from app.core.errors import AuthenticationError
from app.core.security import Principal
from app.database import transaction
from app.services.auth import AuthService


def current_principal(authorization: str | None = Header(default=None)) -> Principal:
    if not authorization or not authorization.startswith("Bearer "):
        raise AuthenticationError("缺少 Bearer 会话令牌")
    token = authorization[7:].strip()
    if not token:
        raise AuthenticationError("会话令牌为空")
    # principal() 会刷新会话最后访问时间；必须在独立显式事务内立即提交，
    # 否则该隐式写事务会让后续端点事务误走 SAVEPOINT 分支而整体无法提交。
    # 使用延迟事务，仅在心跳 UPDATE 时才升级为写锁，避免只读请求抢占写锁。
    with transaction() as connection:
        return AuthService(connection).principal(token)
