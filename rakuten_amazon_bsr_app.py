"""
楽天 × Amazon(SP-API/仮データ) 仕入れ候補抽出アプリ【BSR履歴版】

月間販売数の代わりに、売れ筋ランキング(BSR)のスナップショット履歴で判定する:
  条件A: 直近N回(デフォルト3回)連続で「掲載」(順位がしきい値以内)
  条件B: 前回より順位が上昇(数字が小さくなった)
  ※ A / B は OR / AND を選択可能。加えて 原価率 <= 60% を常に適用。

起動:
    pip install streamlit requests pandas
    streamlit run rakuten_amazon_bsr_app.py

履歴の蓄積:
    スナップショットは「JANごとに1日1件」SQLiteに保存(同日再実行は上書き)。
    つまり「3回」= 3日分。cron等で毎日 take_snapshot を回すと精度が安定する。
"""

import hashlib
import random
import re
import sqlite3
import time
from datetime import date, timedelta

import pandas as pd
import requests
import streamlit as st

DB_PATH = "bsr_history.db"
DEFAULT_RAKUTEN_ENDPOINT = (
    "https://app.rakuten.co.jp/services/api/IchibaItem/Search/20220601"
)
JAN_PATTERN = re.compile(r"(?<!\d)(\d{13})(?!\d)")


# ======================================================================
# 履歴DB (SQLite)
# ======================================================================
def init_db() -> None:
    with sqlite3.connect(DB_PATH) as con:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS snapshots (
                jan TEXT NOT NULL,
                captured_date TEXT NOT NULL,
                asin TEXT,
                rank INTEGER,          -- NULL = ランキング情報なし(圏外)
                price INTEGER,
                PRIMARY KEY (jan, captured_date)
            )
            """
        )


def save_snapshots(rows: list[tuple]) -> None:
    """rows: (jan, captured_date, asin, rank, price)。同日は上書き。"""
    with sqlite3.connect(DB_PATH) as con:
        con.executemany(
            "INSERT OR REPLACE INTO snapshots VALUES (?,?,?,?,?)", rows
        )


def load_history(jans: list[str]) -> pd.DataFrame:
    if not jans:
        return pd.DataFrame(columns=["jan", "captured_date", "asin", "rank", "price"])
    q = ",".join("?" * len(jans))
    with sqlite3.connect(DB_PATH) as con:
        return pd.read_sql_query(
            f"SELECT * FROM snapshots WHERE jan IN ({q}) ORDER BY jan, captured_date",
            con,
            params=jans,
        )


# ======================================================================
# 楽天市場API
# ======================================================================
def extract_jan(item: dict) -> str | None:
    if item.get("janCode"):
        return str(item["janCode"])
    for key in ("itemCaption", "catchcopy", "itemName"):
        m = JAN_PATTERN.search(str(item.get(key, "")))
        if m:
            return m.group(1)
    return None


def mock_rakuten_items(keyword: str, n: int) -> list[dict]:
    rng = random.Random(keyword)
    return [
        {
            "itemName": f"{keyword} サンプル商品 {i + 1}",
            "itemPrice": rng.randint(5, 80) * 100,
            "itemUrl": "https://www.rakuten.co.jp/",
            "shopName": f"サンプルショップ{rng.randint(1, 5)}",
            "janCode": "49" + "".join(str(rng.randint(0, 9)) for _ in range(11)),
        }
        for i in range(n)
    ]


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_rakuten_items(endpoint, app_id, access_key, keyword, pages, use_mock):
    if use_mock:
        return mock_rakuten_items(keyword, 30 * pages)
    items: list[dict] = []
    for page in range(1, pages + 1):
        params = {
            "applicationId": app_id, "keyword": keyword, "hits": 30, "page": page,
            "availability": 1, "sort": "+itemPrice", "format": "json", "formatVersion": 2,
        }
        if access_key:
            params["accessKey"] = access_key
        resp = requests.get(endpoint, params=params, timeout=15)
        resp.raise_for_status()
        batch = resp.json().get("Items", [])
        if not batch:
            break
        items.extend(batch)
    return items


# ======================================================================
# Amazon データ取得（プロバイダ方式）
#   fetch(jans) -> {jan: {"asin":..., "price":int|None, "rank":int|None}}
# ======================================================================
class MockProvider:
    """仮データ。日付ごとに順位が少し変動する。"""

    def fetch(self, jans: list[str], on_date: date | None = None) -> dict[str, dict]:
        on_date = on_date or date.today()
        out = {}
        for jan in jans:
            seed = int(hashlib.md5(jan.encode()).hexdigest(), 16)
            base = random.Random(seed)
            asin = "B0" + "".join(base.choices("ABCDEFGHJKLMNPQRSTUVWXYZ0123456789", k=8))
            price = base.randint(10, 150) * 100
            base_rank = base.choice([80, 300, 800, 2000, 8000, 50000, 200000])
            day = random.Random(seed + on_date.toordinal())
            rank = int(base_rank * day.uniform(0.6, 1.5))
            if day.random() < 0.1:  # たまに圏外
                rank = None
            out[jan] = {"asin": asin, "price": price, "rank": rank}
        return out


class SpApiProvider:
    """
    Amazon SP-API (日本)。※実機未検証のサンプル。仕様は公式ドキュメントで確認すること。
      - Catalog Items API 2022-04-01 : ASIN/ランキング
      - Product Pricing API v0       : 競争価格(カート価格に近い値)
    """

    MARKETPLACE_ID = "A1VC38T7YXB528"  # amazon.co.jp
    ENDPOINT = "https://sellingpartnerapi-fe.amazon.com"

    def __init__(self, client_id: str, client_secret: str, refresh_token: str):
        self.client_id, self.client_secret, self.refresh_token = (
            client_id, client_secret, refresh_token,
        )

    def _token(self) -> str:
        r = requests.post(
            "https://api.amazon.com/auth/o2/token",
            data={
                "grant_type": "refresh_token",
                "refresh_token": self.refresh_token,
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            },
            timeout=15,
        )
        r.raise_for_status()
        return r.json()["access_token"]

    @staticmethod
    def _pick_rank(item: dict) -> int | None:
        for sr in item.get("salesRanks", []):
            for key in ("displayGroupRanks", "classificationRanks"):
                ranks = sr.get(key) or []
                if ranks:
                    return int(ranks[0]["rank"])
        return None

    def fetch(self, jans: list[str], on_date: date | None = None) -> dict[str, dict]:
        headers = {"x-amz-access-token": self._token()}
        out: dict[str, dict] = {}

        # 1) JAN(EAN) -> ASIN / ランキング (最大20件/回)
        for i in range(0, len(jans), 20):
            chunk = jans[i : i + 20]
            r = requests.get(
                f"{self.ENDPOINT}/catalog/2022-04-01/items",
                headers=headers,
                params={
                    "identifiers": ",".join(chunk),
                    "identifiersType": "EAN",
                    "marketplaceIds": self.MARKETPLACE_ID,
                    "includedData": "identifiers,salesRanks",
                },
                timeout=30,
            )
            r.raise_for_status()
            for item in r.json().get("items", []):
                eans = {
                    idf["identifier"]
                    for grp in item.get("identifiers", [])
                    for idf in grp.get("identifiers", [])
                    if idf.get("identifierType") == "EAN"
                }
                for jan in chunk:
                    if jan in eans:
                        out[jan] = {"asin": item["asin"], "price": None,
                                    "rank": self._pick_rank(item)}
            time.sleep(0.6)  # レート制限対策

        # 2) ASIN -> 価格 (最大20件/回)
        asin_to_jan = {v["asin"]: j for j, v in out.items()}
        asins = list(asin_to_jan)
        for i in range(0, len(asins), 20):
            chunk = asins[i : i + 20]
            r = requests.get(
                f"{self.ENDPOINT}/products/pricing/v0/competitivePrice",
                headers=headers,
                params={"MarketplaceId": self.MARKETPLACE_ID,
                        "Asins": ",".join(chunk), "ItemType": "Asin"},
                timeout=30,
            )
            r.raise_for_status()
            for p in r.json().get("payload", []):
                try:
                    prices = p["Product"]["CompetitivePricing"]["CompetitivePrices"]
                    amount = prices[0]["Price"]["LandedPrice"]["Amount"]
                    out[asin_to_jan[p["ASIN"]]]["price"] = int(amount)
                except (KeyError, IndexError, TypeError):
                    pass
            time.sleep(2.1)  # このAPIは約0.5 req/s
        return out


# ======================================================================
# BSR履歴の評価ロジック
# ======================================================================
def evaluate_history(
    hist: pd.DataFrame,
    rank_threshold: int,
    n_consecutive: int,
    min_improve_pct: float,
) -> pd.DataFrame:
    """JANごとに 条件A(連続掲載) / 条件B(順位上昇) を判定する。"""
    rows = []
    for jan, g in hist.groupby("jan"):
        ranks = [None if pd.isna(r) else int(r) for r in g.sort_values("captured_date")["rank"]]
        latest = ranks[-1] if ranks else None
        prev = ranks[-2] if len(ranks) >= 2 else None

        listed = lambda r: r is not None and r <= rank_threshold  # noqa: E731

        # 条件A: 直近N回すべて掲載(履歴がN件未満なら不成立)
        cond_a = len(ranks) >= n_consecutive and all(listed(r) for r in ranks[-n_consecutive:])

        # 条件B: 前回より上昇。前回が圏外で今回ランクインも上昇扱い。
        #        ゴミ順位同士の変動を避けるため、最新がしきい値以内であることも要求。
        cond_b = False
        if listed(latest) and prev is not None:
            cond_b = latest <= prev * (1 - min_improve_pct / 100)
        elif listed(latest) and len(ranks) >= 2 and prev is None:
            cond_b = True

        rows.append(
            {
                "jan": jan,
                "履歴件数": len(ranks),
                "最新順位": latest,
                "前回順位": prev,
                f"直近{n_consecutive}回連続掲載": cond_a,
                "前回より上昇": cond_b,
            }
        )
    return pd.DataFrame(rows)


# ======================================================================
# UI
# ======================================================================
st.set_page_config(page_title="楽天×Amazon BSR履歴 仕入れ候補", layout="wide")
st.title("📈 楽天 × Amazon BSR履歴 仕入れ候補抽出")
init_db()

with st.sidebar:
    st.header("API設定")
    use_mock_rakuten = st.checkbox("楽天: 仮データを使う", value=True)
    rakuten_endpoint = st.text_input("楽天 エンドポイント", DEFAULT_RAKUTEN_ENDPOINT)
    rakuten_app_id = st.text_input("楽天 applicationId", type="password")
    rakuten_access_key = st.text_input("楽天 accessKey（必要な場合）", type="password")

    use_mock_amazon = st.checkbox("Amazon: 仮データを使う", value=True)
    sp_client_id = st.text_input("SP-API Client ID", type="password")
    sp_client_secret = st.text_input("SP-API Client Secret", type="password")
    sp_refresh_token = st.text_input("SP-API Refresh Token", type="password")

    st.header("価格条件")
    max_ratio = st.slider("仕入れ値 ÷ 販売価格 の上限", 0.1, 1.0, 0.60, 0.01)
    fee_rate = st.slider("Amazon手数料率（利益試算用）", 0.0, 0.3, 0.15, 0.01)

    st.header("ランキング条件")
    rank_threshold = st.number_input("「掲載」とみなす順位のしきい値(以内)", 1, 1_000_000, 3000, 100)
    n_consecutive = st.number_input("連続掲載の回数(日数)", 2, 14, 3)
    min_improve = st.slider("上昇とみなす最小改善率(%)", 0, 50, 0)
    combine = st.radio("条件Aと条件Bの結合", ["OR（どちらか）", "AND（両方）"])

    pages = st.number_input("楽天の取得ページ数(1ページ30件)", 1, 10, 2)
    seed_demo = st.checkbox("仮データ時: 過去5日分の履歴を自動生成(デモ用)", value=True)

keyword = st.text_input("楽天検索キーワード", "ワイヤレスイヤホン")

if st.button("検索して抽出（今日のスナップショットも保存）", type="primary"):
    try:
        with st.spinner("楽天市場から取得中..."):
            items = fetch_rakuten_items(
                rakuten_endpoint, rakuten_app_id, rakuten_access_key,
                keyword, int(pages), use_mock_rakuten,
            )
        jan_to_item = {}
        for it in items:
            jan = extract_jan(it)
            if jan and jan not in jan_to_item:
                jan_to_item[jan] = it
        jans = list(jan_to_item)
        st.write(f"楽天: {len(items)} 件取得 / JAN特定: {len(jans)} 件")

        provider = (
            MockProvider() if use_mock_amazon
            else SpApiProvider(sp_client_id, sp_client_secret, sp_refresh_token)
        )

        # 仮データなら過去履歴をデモ用に生成
        if use_mock_amazon and seed_demo:
            today = date.today()
            for d in range(5, 0, -1):
                day = today - timedelta(days=d)
                snap = provider.fetch(jans, on_date=day)
                save_snapshots([
                    (j, day.isoformat(), v["asin"], v["rank"], v["price"])
                    for j, v in snap.items()
                ])

        with st.spinner("Amazonから価格・ランキングを取得中..."):
            snap = provider.fetch(jans)
        today_str = date.today().isoformat()
        save_snapshots([
            (j, today_str, v["asin"], v["rank"], v["price"]) for j, v in snap.items()
        ])

        # --- 評価 ---
        hist = load_history(list(snap))
        ev = evaluate_history(hist, int(rank_threshold), int(n_consecutive), float(min_improve))
        col_a = f"直近{int(n_consecutive)}回連続掲載"

        rows = []
        for _, e in ev.iterrows():
            it, s = jan_to_item[e["jan"]], snap[e["jan"]]
            if not s.get("price"):
                continue
            cost, sell = int(it["itemPrice"]), int(s["price"])
            rows.append({
                "商品名(楽天)": it.get("itemName"),
                "JAN": e["jan"],
                "仕入れ値": cost,
                "Amazon価格": sell,
                "原価率": round(cost / sell, 3),
                "推定利益": int(sell - sell * fee_rate - cost),
                "最新順位": e["最新順位"],
                "前回順位": e["前回順位"],
                "履歴件数": e["履歴件数"],
                col_a: e[col_a],
                "前回より上昇": e["前回より上昇"],
                "ASIN": s.get("asin"),
                "楽天URL": it.get("itemUrl"),
                "Amazon URL": f"https://www.amazon.co.jp/dp/{s.get('asin')}",
            })
        df = pd.DataFrame(rows)

        if df.empty:
            st.warning("突合できる商品がありませんでした。")
        else:
            rank_ok = (
                (df[col_a] | df["前回より上昇"]) if combine.startswith("OR")
                else (df[col_a] & df["前回より上昇"])
            )
            hit = df[(df["原価率"] <= max_ratio) & rank_ok].sort_values(
                "推定利益", ascending=False
            )
            c1, c2, c3 = st.columns(3)
            c1.metric("突合できた商品", len(df))
            c2.metric("条件に合致", len(hit))
            c3.metric("最大推定利益", f"¥{hit['推定利益'].max():,}" if len(hit) else "-")

            if df["履歴件数"].max() < n_consecutive:
                st.info(
                    f"履歴がまだ{int(n_consecutive)}日分ありません。条件A(連続掲載)は"
                    "履歴が溜まるまで成立しません。毎日実行して蓄積してください。"
                )

            st.subheader("✅ 抽出結果")
            cfg = {
                "楽天URL": st.column_config.LinkColumn("楽天", display_text="開く"),
                "Amazon URL": st.column_config.LinkColumn("Amazon", display_text="開く"),
            }
            st.dataframe(hit, use_container_width=True, hide_index=True, column_config=cfg)
            st.download_button(
                "CSVダウンロード", hit.to_csv(index=False).encode("utf-8-sig"),
                file_name="candidates.csv", mime="text/csv",
            )
            with st.expander("突合できた全商品を見る"):
                st.dataframe(df, use_container_width=True, hide_index=True, column_config=cfg)

    except requests.HTTPError as e:
        st.error(f"APIエラー: {e}")
    except Exception as e:  # noqa: BLE001
        st.error(f"エラー: {e}")
