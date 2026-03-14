import re
from dataclasses import dataclass
from datetime import datetime
from typing import Optional


def parse_datetime(val: str) -> datetime:
    val = val.replace("Z", "+00:00")
    # Normalize microseconds to 6 digits
    match = re.match(r"(.+\.\d+)(\+.+)", val)
    if match:
        base, tz = match.groups()
        parts = base.split(".")
        if len(parts) == 2:
            micros = parts[1].ljust(6, "0")[:6]
            val = f"{parts[0]}.{micros}{tz}"
    return datetime.fromisoformat(val)


@dataclass
class Trade:
    trade_id: str
    ticker: str
    count: int
    yes_price: int
    no_price: int
    taker_side: str
    created_time: datetime
    price: float  # Fractional price (0.0-1.0) - convenience field from API

    @classmethod
    def from_dict(cls, data: dict) -> "Trade":
        return cls(
            trade_id=data["trade_id"],
            ticker=data["ticker"],
            count=data["count"],
            yes_price=data["yes_price"],
            no_price=data["no_price"],
            taker_side=data["taker_side"],
            created_time=parse_datetime(data["created_time"]),
            price=data.get("price", data["yes_price"] / 100.0),  # Fractional format
        )


@dataclass
class Market:
    # Core identifiers
    ticker: str
    event_ticker: str
    market_type: str
    
    # Descriptions
    title: str
    subtitle: str
    yes_sub_title: str
    no_sub_title: str
    
    # Status and result
    status: str
    result: str
    
    # Current prices
    yes_bid: Optional[int]
    yes_ask: Optional[int]
    no_bid: Optional[int]
    no_ask: Optional[int]
    last_price: Optional[int]
    
    # Previous prices (for change analysis)
    previous_price: Optional[int]
    previous_yes_bid: Optional[int]
    previous_yes_ask: Optional[int]
    
    # Volume and liquidity
    volume: int
    volume_24h: int
    open_interest: int
    liquidity: int
    
    # Market structure
    tick_size: int
    strike_type: str
    can_close_early: bool
    is_provisional: bool
    
    # Rules and settlement
    rules_primary: str
    rules_secondary: str
    expiration_value: str
    
    # Timestamps
    created_time: Optional[datetime]
    open_time: Optional[datetime]
    close_time: Optional[datetime]
    updated_time: Optional[datetime]
    expected_expiration_time: Optional[datetime]
    expiration_time: Optional[datetime]
    latest_expiration_time: Optional[datetime]
    
    # Multivariate/combo markets
    mve_collection_ticker: Optional[str]
    mve_selected_legs: Optional[str]  # JSON string

    @classmethod
    def from_dict(cls, data: dict) -> "Market":
        def parse_time(val: Optional[str]) -> Optional[datetime]:
            if not val:
                return None
            return parse_datetime(val)
        
        # Convert mve_selected_legs list to JSON string for storage
        mve_legs = data.get("mve_selected_legs")
        mve_legs_str = None
        if mve_legs:
            import json
            mve_legs_str = json.dumps(mve_legs)

        return cls(
            # Core identifiers
            ticker=data["ticker"],
            event_ticker=data["event_ticker"],
            market_type=data.get("market_type", "binary"),
            
            # Descriptions
            title=data.get("title", ""),
            subtitle=data.get("subtitle", ""),
            yes_sub_title=data.get("yes_sub_title", ""),
            no_sub_title=data.get("no_sub_title", ""),
            
            # Status and result
            status=data["status"],
            result=data.get("result", ""),
            
            # Current prices
            yes_bid=data.get("yes_bid"),
            yes_ask=data.get("yes_ask"),
            no_bid=data.get("no_bid"),
            no_ask=data.get("no_ask"),
            last_price=data.get("last_price"),
            
            # Previous prices
            previous_price=data.get("previous_price"),
            previous_yes_bid=data.get("previous_yes_bid"),
            previous_yes_ask=data.get("previous_yes_ask"),
            
            # Volume and liquidity
            volume=data.get("volume", 0),
            volume_24h=data.get("volume_24h", 0),
            open_interest=data.get("open_interest", 0),
            liquidity=data.get("liquidity", 0),
            
            # Market structure
            tick_size=data.get("tick_size", 1),
            strike_type=data.get("strike_type", ""),
            can_close_early=data.get("can_close_early", False),
            is_provisional=data.get("is_provisional", False),
            
            # Rules and settlement
            rules_primary=data.get("rules_primary", ""),
            rules_secondary=data.get("rules_secondary", ""),
            expiration_value=data.get("expiration_value", ""),
            
            # Timestamps
            created_time=parse_time(data.get("created_time")),
            open_time=parse_time(data.get("open_time")),
            close_time=parse_time(data.get("close_time")),
            updated_time=parse_time(data.get("updated_time")),
            expected_expiration_time=parse_time(data.get("expected_expiration_time")),
            expiration_time=parse_time(data.get("expiration_time")),
            latest_expiration_time=parse_time(data.get("latest_expiration_time")),
            
            # Multivariate/combo markets
            mve_collection_ticker=data.get("mve_collection_ticker"),
            mve_selected_legs=mve_legs_str,
        )
