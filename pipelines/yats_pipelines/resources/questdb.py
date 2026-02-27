from dataclasses import dataclass, field


@dataclass
class QuestDBResource:
    """QuestDB resource — PG wire for reads, ILP for writes."""

    pg_host: str = "localhost"
    pg_port: int = 8812
    pg_user: str = "admin"
    pg_password: str = "quest"
    pg_database: str = "qdb"
    ilp_host: str = "localhost"
    ilp_port: int = 9009
