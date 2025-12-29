import os
import smtplib
import hashlib
import json
from email.mime.text import MIMEText
from datetime import datetime, date

import streamlit as st
import pandas as pd
from sqlalchemy import (
    create_engine, MetaData, Table, Column, Integer, String, Float, Date, DateTime,
    ForeignKey, Boolean, Text, Enum, Numeric, select, func, and_, or_, text as sqltext
)
from sqlalchemy.exc import IntegrityError

# ---------------------------
# CONFIG / DB CONNECTION
# ---------------------------
st.set_page_config(page_title="WMS + Procurement", layout="wide")

# ✅ Main page title (top of main page)
st.title("Warehouse Management + Procurement System")
st.caption("WMS • Procurement • Inventory • Deliveries • Analytics")
st.divider()

DB_HOST = st.secrets.get("DB_HOST", os.getenv("DB_HOST", "")).strip()

# If DB_HOST is provided -> MySQL, else -> SQLite (demo mode)
USING_MYSQL = bool(DB_HOST)

if USING_MYSQL:
    DB_PORT = int(st.secrets.get("DB_PORT", os.getenv("DB_PORT", "3306")))
    DB_USER = st.secrets.get("DB_USER", os.getenv("DB_USER", "")).strip()
    DB_PASS = st.secrets.get("DB_PASS", os.getenv("DB_PASS", "")).strip()
    DB_SCHEMA = st.secrets.get("DB_SCHEMA", os.getenv("DB_SCHEMA", "procurement_db")).strip()

    if not (DB_USER and DB_PASS and DB_SCHEMA):
        st.error("MySQL is selected (DB_HOST is set) but DB_USER/DB_PASS/DB_SCHEMA is missing.")
        st.stop()

    CONN_STR = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_SCHEMA}"
else:
    # SQLite file stored in the app directory (good for Streamlit Cloud demo)
    CONN_STR = "sqlite:///pwls.db"

engine = create_engine(CONN_STR, future=True)

# ---------------------------
# SQLAlchemy Core metadata
# ---------------------------
metadata = MetaData()

# JSON column compatibility:
# - MySQL supports JSON type
# - SQLite doesn't (store JSON as Text)
if USING_MYSQL:
    from sqlalchemy.dialects.mysql import JSON as JSON_COL
else:
    JSON_COL = Text

# ---------------------------
# DEMO LOGIN (free access)
# One click only.

# ---------------------------
DEMO_EMAIL = "just_for@demo.com"
DEMO_PASSWORD = "demo1111!"
DEMO_NAME = "Admin just for Demo purposes"
DEMO_ROLE = "admin" 


# ---------------------------
# TABLES

users = Table("users", metadata,
    Column("id", Integer, primary_key=True),
    Column("name", String(120), nullable=False),
    Column("email", String(160), unique=True),
    Column("password_hash", String(128), nullable=False),
    Column("role", Enum("requester","approver","buyer","inspector","finance","admin", name="role_enum"),
           nullable=False, default="requester"),
    Column("dept", String(80)),
    Column("created_at", DateTime, default=datetime.utcnow),
)

suppliers = Table("suppliers", metadata,
    Column("id", Integer, primary_key=True),
    Column("name", String(160), nullable=False),
    Column("contact", String(160)),
    Column("email", String(160)),
    Column("phone", String(40)),
    Column("status", Enum("active","inactive", name="supplier_status"), default="active"),
    Column("rating", Numeric(5,2), default=0),
    Column("created_at", DateTime, default=datetime.utcnow),
)

items = Table("items", metadata,
    Column("id", Integer, primary_key=True),
    Column("sku", String(60)),
    Column("name", String(160), nullable=False),
    Column("uom", String(20), default="ea"),
    Column("last_price", Numeric(12,2), default=0),
    Column("created_at", DateTime, default=datetime.utcnow),
)

requisitions = Table("requisitions", metadata,
    Column("id", Integer, primary_key=True),
    Column("req_no", String(30), unique=True),
    Column("requester_id", Integer, ForeignKey("users.id")),
    Column("needed_by", Date),
    Column("purpose", String(255)),
    Column("status", Enum("draft","pending_approval","approved","rejected","sourced","ordered","delivered","closed",
                         name="req_status"), default="draft"),
    Column("created_at", DateTime, default=datetime.utcnow),
)

requisition_items = Table("requisition_items", metadata,
    Column("id", Integer, primary_key=True),
    Column("requisition_id", Integer, ForeignKey("requisitions.id", ondelete="CASCADE")),
    Column("item_id", Integer, ForeignKey("items.id"), nullable=True),
    Column("description", String(255)),
    Column("qty", Numeric(12,2), nullable=False),
    Column("target_price", Numeric(12,2)),
)

approvals = Table("approvals", metadata,
    Column("id", Integer, primary_key=True),
    Column("requisition_id", Integer, ForeignKey("requisitions.id", ondelete="CASCADE")),
    Column("approver_id", Integer, ForeignKey("users.id")),
    Column("decision", Enum("pending","approved","rejected", name="decision_enum"), default="pending"),
    Column("comments", String(255)),
    Column("decided_at", DateTime, nullable=True),
)

rfqs = Table("rfqs", metadata,
    Column("id", Integer, primary_key=True),
    Column("requisition_id", Integer, ForeignKey("requisitions.id")),
    Column("rfq_no", String(30), unique=True),
    Column("status", Enum("draft","sent","closed", name="rfq_status"), default="draft"),
    Column("created_at", DateTime, default=datetime.utcnow),
)

rfq_items = Table("rfq_items", metadata,
    Column("id", Integer, primary_key=True),
    Column("rfq_id", Integer, ForeignKey("rfqs.id", ondelete="CASCADE")),
    Column("item_desc", String(255)),
    Column("qty", Numeric(12,2), nullable=False),
)

bids = Table("bids", metadata,
    Column("id", Integer, primary_key=True),
    Column("rfq_id", Integer, ForeignKey("rfqs.id", ondelete="CASCADE")),
    Column("supplier_id", Integer, ForeignKey("suppliers.id")),
    Column("price", Numeric(12,2), nullable=False),
    Column("lead_time_days", Integer, default=0),
    Column("notes", String(255)),
)

purchase_orders = Table("purchase_orders", metadata,
    Column("id", Integer, primary_key=True),
    Column("po_no", String(30), unique=True),
    Column("supplier_id", Integer, ForeignKey("suppliers.id")),
    Column("rfq_id", Integer, ForeignKey("rfqs.id")),
    Column("status", Enum("created","acknowledged","partially_delivered","delivered","closed", name="po_status"),
           default="created"),
    Column("created_at", DateTime, default=datetime.utcnow),
)

po_items = Table("po_items", metadata,
    Column("id", Integer, primary_key=True),
    Column("po_id", Integer, ForeignKey("purchase_orders.id", ondelete="CASCADE")),
    Column("item_desc", String(255)),
    Column("qty", Numeric(12,2), nullable=False),
    Column("unit_price", Numeric(12,2), nullable=False),
)

goods_receipts = Table("goods_receipts", metadata,
    Column("id", Integer, primary_key=True),
    Column("po_id", Integer, ForeignKey("purchase_orders.id")),
    Column("grn_no", String(30), unique=True),
    Column("received_at", Date),
    Column("received_by", Integer, ForeignKey("users.id")),
)

inspections = Table("inspections", metadata,
    Column("id", Integer, primary_key=True),
    Column("grn_id", Integer, ForeignKey("goods_receipts.id", ondelete="CASCADE")),
    Column("result", Enum("accepted","rejected","accepted_with_notes", name="insp_result"), default="accepted"),
    Column("notes", String(255)),
    Column("inspected_by", Integer, ForeignKey("users.id")),
    Column("inspected_at", DateTime, default=datetime.utcnow),
)

invoices = Table("invoices", metadata,
    Column("id", Integer, primary_key=True),
    Column("supplier_id", Integer, ForeignKey("suppliers.id")),
    Column("po_id", Integer, ForeignKey("purchase_orders.id")),
    Column("invoice_no", String(40), unique=True),
    Column("amount", Numeric(12,2)),
    Column("status", Enum("received","matched","paid","rejected", name="inv_status"), default="received"),
    Column("received_at", Date),
)

invoice_items = Table("invoice_items", metadata,
    Column("id", Integer, primary_key=True),
    Column("invoice_id", Integer, ForeignKey("invoices.id", ondelete="CASCADE")),
    Column("item_desc", String(255)),
    Column("qty", Numeric(12,2), nullable=False),
    Column("unit_price", Numeric(12,2), nullable=False),
)

payments = Table("payments", metadata,
    Column("id", Integer, primary_key=True),
    Column("invoice_id", Integer, ForeignKey("invoices.id", ondelete="CASCADE")),
    Column("paid_at", Date),
    Column("method", String(40)),
    Column("reference", String(80)),
)

documents = Table("documents", metadata,
    Column("id", Integer, primary_key=True),
    Column("entity_type", String(30)),
    Column("entity_id", Integer),
    Column("filename", String(160)),
    Column("path", String(255)),
    Column("uploaded_at", DateTime, default=datetime.utcnow),
)

supplier_scores = Table("supplier_scores", metadata,
    Column("id", Integer, primary_key=True),
    Column("supplier_id", Integer, ForeignKey("suppliers.id")),
    Column("score_date", Date),
    Column("quality", Integer),
    Column("delivery", Integer),
    Column("service", Integer),
    Column("overall", Numeric(5,2)),
    Column("notes", String(255)),
)

audit_logs = Table("audit_logs", metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("actor", String(160)),
    Column("action", String(80)),
    Column("entity_type", String(30)),
    Column("entity_id", Integer),
    Column("details", JSON_COL),
    Column("created_at", DateTime, default=datetime.utcnow),
)

warehouses = Table("warehouses", metadata,
    Column("id", Integer, primary_key=True),
    Column("name", String(120), nullable=False),
    Column("code", String(40), unique=True),
    Column("address", String(255)),
    Column("created_at", DateTime, default=datetime.utcnow),
)

bin_locations = Table("bin_locations", metadata,
    Column("id", Integer, primary_key=True),
    Column("warehouse_id", Integer, ForeignKey("warehouses.id", ondelete="CASCADE")),
    Column("code", String(60), nullable=False),
    Column("desc", String(160)),
)

inventory = Table("inventory", metadata,
    Column("id", Integer, primary_key=True),
    Column("warehouse_id", Integer, ForeignKey("warehouses.id")),
    Column("item_id", Integer, ForeignKey("items.id")),
    Column("bin_id", Integer, ForeignKey("bin_locations.id"), nullable=True),
    Column("qty", Numeric(14,3), default=0),
    Column("updated_at", DateTime, default=datetime.utcnow, onupdate=datetime.utcnow),
)

stock_movements = Table("stock_movements", metadata,
    Column("id", Integer, primary_key=True),
    Column("movement_no", String(30), unique=True),
    Column("movement_type", Enum("IN","OUT","TRANSFER", name="movement_type")),
    Column("warehouse_id", Integer, ForeignKey("warehouses.id")),
    Column("warehouse_to_id", Integer, ForeignKey("warehouses.id"), nullable=True),
    Column("item_id", Integer, ForeignKey("items.id")),
    Column("bin_id", Integer, ForeignKey("bin_locations.id"), nullable=True),
    Column("qty", Numeric(14,3)),
    Column("reason", String(160)),
    Column("ref_entity", String(30)),
    Column("ref_id", Integer),
    Column("created_at", DateTime, default=datetime.utcnow),
)

deliveries = Table("deliveries", metadata,
    Column("id", Integer, primary_key=True),
    Column("do_no", String(30), unique=True),
    Column("warehouse_id", Integer, ForeignKey("warehouses.id")),
    Column("customer_name", String(160)),
    Column("status", Enum("created","picked","shipped","delivered", name="delivery_status"), default="created"),
    Column("scheduled_date", Date),
    Column("created_at", DateTime, default=datetime.utcnow),
)

delivery_items = Table("delivery_items", metadata,
    Column("id", Integer, primary_key=True),
    Column("delivery_id", Integer, ForeignKey("deliveries.id", ondelete="CASCADE")),
    Column("item_id", Integer, ForeignKey("items.id")),
    Column("bin_id", Integer, ForeignKey("bin_locations.id"), nullable=True),
    Column("qty", Numeric(14,3)),
)

vehicles = Table("vehicles", metadata,
    Column("id", Integer, primary_key=True),
    Column("plate_no", String(40), unique=True),
    Column("capacity_kg", Numeric(12,3)),
    Column("active", Boolean, default=True),
)

drivers = Table("drivers", metadata,
    Column("id", Integer, primary_key=True),
    Column("name", String(120)),
    Column("phone", String(60)),
    Column("active", Boolean, default=True),
)

delivery_assignments = Table("delivery_assignments", metadata,
    Column("id", Integer, primary_key=True),
    Column("delivery_id", Integer, ForeignKey("deliveries.id", ondelete="CASCADE")),
    Column("vehicle_id", Integer, ForeignKey("vehicles.id")),
    Column("driver_id", Integer, ForeignKey("drivers.id")),
    Column("assigned_at", DateTime, default=datetime.utcnow),
)

# ---------------------------
# DB schema safely Initialised

def init_db():
    try:
        metadata.create_all(engine)
        return True, ""
    except Exception as e:
        return False, str(e)

ok, err = init_db()
if not ok:
    st.error("Database initialization failed.")
    st.code(err)
    st.info(
        "If deploying on Streamlit Cloud without MySQL, leave Secrets empty to use SQLite. "
        "If using MySQL, set DB_HOST/DB_USER/DB_PASS/DB_SCHEMA in Streamlit Secrets to a hosted DB (not 127.0.0.1)."
    )
    st.stop()

# ---------------------------
# HELPERS
# ---------------------------
def sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()

def now_ts_id(prefix: str) -> str:
    return datetime.now().strftime(f"{prefix}%Y%m%d%H%M%S")

def log(actor, action, entity_type, entity_id, details=None):
    payload = details or {}
    if not USING_MYSQL:
        payload = json.dumps(payload, ensure_ascii=False)
    with engine.begin() as conn:
        conn.execute(audit_logs.insert().values(
            actor=actor, action=action, entity_type=entity_type,
            entity_id=entity_id, details=payload
        ))

# ✅ demo user in SQLite demo mode (only if no users exist)
def ensure_demo_user_exists():
    with engine.begin() as conn:
        u = conn.execute(
            select(users).where(users.c.email == DEMO_EMAIL)
        ).mappings().first()

        if not u:
            conn.execute(users.insert().values(
                name=DEMO_NAME,
                email=DEMO_EMAIL,
                password_hash=sha256(DEMO_PASSWORD),
                role=DEMO_ROLE,
                dept="Demo",
            ))


ensure_demo_user_exists()

# Email (optional) - from environment only (safe for Cloud)
SMTP_HOST = os.getenv("SMTP_HOST", "")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASS = os.getenv("SMTP_PASS", "")
EMAIL_FROM = os.getenv("EMAIL_FROM", "")

def send_email(to_email: str, subject: str, body: str):
    if not (SMTP_HOST and SMTP_PORT and SMTP_USER and SMTP_PASS and EMAIL_FROM):
        return False, "SMTP not configured"
    try:
        msg = MIMEText(body)
        msg["Subject"] = subject
        msg["From"] = EMAIL_FROM
        msg["To"] = to_email
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as s:
            s.starttls()
            s.login(SMTP_USER, SMTP_PASS)
            s.sendmail(EMAIL_FROM, [to_email], msg.as_string())
        return True, "sent"
    except Exception as e:
        return False, str(e)

def require_role(roles: list[str]) -> bool:
    u = st.session_state.get("user")
    return bool(u and (u["role"] in roles or u["role"] == "admin"))



# Inventory utilities
def get_or_create_inv(conn, warehouse_id: int, item_id: int, bin_id: int|None = None):
    row = conn.execute(select(inventory).where(
        and_(inventory.c.warehouse_id==warehouse_id,
             inventory.c.item_id==item_id,
             inventory.c.bin_id.is_(bin_id))
    )).mappings().first()
    if row:
        return row
    conn.execute(inventory.insert().values(warehouse_id=warehouse_id, item_id=item_id, bin_id=bin_id, qty=0))
    return conn.execute(select(inventory).where(
        and_(inventory.c.warehouse_id==warehouse_id,
             inventory.c.item_id==item_id,
             inventory.c.bin_id.is_(bin_id))
    )).mappings().first()

def inventory_in(conn, warehouse_id: int, item_id: int, qty: float, ref_entity: str, ref_id: int, bin_id: int|None=None):
    row = get_or_create_inv(conn, warehouse_id, item_id, bin_id)
    new_qty = float(row["qty"]) + float(qty)
    conn.execute(inventory.update().where(inventory.c.id==row["id"]).values(qty=new_qty))
    conn.execute(stock_movements.insert().values(
        movement_no=now_ts_id("MIN"),
        movement_type="IN", warehouse_id=warehouse_id, warehouse_to_id=None,
        item_id=item_id, bin_id=bin_id, qty=qty, reason="GRN", ref_entity=ref_entity, ref_id=ref_id
    ))

def inventory_out(conn, warehouse_id: int, item_id: int, qty: float, ref_entity: str, ref_id: int, bin_id: int|None=None):
    row = get_or_create_inv(conn, warehouse_id, item_id, bin_id)
    new_qty = float(row["qty"]) - float(qty)
    if new_qty < -1e-6:
        raise ValueError(f"Insufficient stock: item {item_id} in WH {warehouse_id} has {row['qty']} < {qty}")
    conn.execute(inventory.update().where(inventory.c.id==row["id"]).values(qty=new_qty))
    conn.execute(stock_movements.insert().values(
        movement_no=now_ts_id("MOUT"),
        movement_type="OUT", warehouse_id=warehouse_id, warehouse_to_id=None,
        item_id=item_id, bin_id=bin_id, qty=qty, reason="DELIVERY", ref_entity=ref_entity, ref_id=ref_id
    ))

def transfer_stock(conn, wh_from: int, wh_to: int, item_id: int, qty: float, reason="TRANSFER"):
    inventory_out(conn, wh_from, item_id, qty, "TRANSFER", 0)
    inventory_in(conn, wh_to, item_id, qty, "TRANSFER", 0)
    conn.execute(stock_movements.insert().values(
        movement_no=now_ts_id("MTR"),
        movement_type="TRANSFER", warehouse_id=wh_from, warehouse_to_id=wh_to,
        item_id=item_id, qty=qty, reason=reason, ref_entity="TRANSFER", ref_id=0
    ))

# ---------------------------
# AUTH / SESSION

# ---------------------------
# AUTH / SESSION (ONE-CLICK DEMO LOGIN ONLY)
if "user" not in st.session_state:
    st.session_state.user = None

with st.sidebar:
    st.subheader("Account")

    if st.session_state.user:
        st.success(f"Signed in as {st.session_state.user['name']} ({st.session_state.user['role']})")
        if st.button("Sign out"):
            st.session_state.user = None
            st.rerun()
    else:
        # one-click access (no email/password fields)
        st.caption("Free access")
        if st.button("Demo login"):
            with engine.begin() as conn:
                u = conn.execute(select(users).where(users.c.email == DEMO_EMAIL)).mappings().first()

            if not u:
                st.error("Demo user not found. Check ensure_demo_user_exists().")
            else:
                st.session_state.user = {"id": u["id"], "name": u["name"], "role": u["role"], "email": u["email"]}
                st.rerun()

if not st.session_state.user:
    st.info("Click Demo login to continue.")
    st.stop()

# Block the app until logged in
if not st.session_state.user:
    st.info("Click **Demo login** in the sidebar to access the app.")
    st.stop()



# ---------------------------
# SIDEBAR NAV (role-aware)
# ---------------------------
def menu_item(label, allowed_roles):
    if require_role(allowed_roles):
        return label
    return None

nav_all = [
    menu_item("1) Requisitions", ["requester","buyer"]),
    menu_item("2) Approvals", ["approver","admin"]),
    menu_item("3) RFQs & Bids", ["buyer"]),
    menu_item("4) Purchase Orders", ["buyer"]),
    menu_item("5) GRN & Inspection (auto-inventory IN)", ["inspector","buyer"]),
    menu_item("6) Invoices & Payments (3-way match)", ["finance","buyer"]),
    menu_item("7) Suppliers & Scores", ["buyer","inspector"]),
    menu_item("8) Warehouses", ["buyer","admin"]),
    menu_item("9) Inventory & Transfers", ["buyer","inspector","admin"]),
    menu_item("10) Outbound Deliveries (auto-inventory OUT)", ["buyer","admin"]),
    menu_item("11) Vehicles & Drivers", ["admin","buyer"]),
    menu_item("12) Documents", ["buyer","finance","inspector","requester"]),
    menu_item("13) Reports & Analytics (incl. valuation)", ["buyer","finance","admin"]),
    menu_item("Admin: Users", ["admin"]),
]
nav = [x for x in nav_all if x]
choice = st.sidebar.radio("Navigate", nav, index=0)

# ---------------------------------
# PAGES
# ---------------------------------
def page_requisitions():
    st.header("Requisitions")
    col1, col2 = st.columns(2)
    with col1:
        requester_id = st.number_input("Requester user ID", min_value=1, step=1, value=st.session_state.user["id"])
        needed_by = st.date_input("Needed by")
        purpose = st.text_input("Purpose / justification")
        if st.button("Create Requisition"):
            req_no = now_ts_id("REQ")
            with engine.begin() as conn:
                rid = conn.execute(requisitions.insert().values(
                    req_no=req_no, requester_id=requester_id, needed_by=needed_by,
                    purpose=purpose, status="pending_approval"
                )).inserted_primary_key[0]
            log(st.session_state.user["email"], "CREATE", "REQ", rid, {"req_no": req_no})
            st.success(f"Requisition {req_no} created (ID {rid})")
    with col2:
        st.subheader("Add items to a requisition")
        rid = st.number_input("Requisition ID", min_value=1, step=1, key="rid_add")
        desc = st.text_input("Item description")
        qty = st.number_input("Qty", min_value=0.0, step=1.0)
        target = st.number_input("Target unit price", min_value=0.0, step=0.01)
        if st.button("Add Item"):
            with engine.begin() as conn:
                conn.execute(requisition_items.insert().values(
                    requisition_id=int(rid), item_id=None, description=desc, qty=qty, target_price=target
                ))
            st.success("Item added")
    st.subheader("My Requisitions (latest)")
    with engine.begin() as conn:
        df = pd.read_sql_query(sqltext("""
            SELECT r.id, r.req_no, r.purpose, r.status, r.needed_by, u.name AS requester,
                   (SELECT COUNT(*) FROM requisition_items ri WHERE ri.requisition_id=r.id) AS items
            FROM requisitions r LEFT JOIN users u ON u.id=r.requester_id
            ORDER BY r.id DESC LIMIT 200
        """), conn)
    st.dataframe(df, use_container_width=True)

def page_approvals():
    st.header("Approvals")
    rid = st.number_input("Requisition ID", min_value=1, step=1)
    decision = st.selectbox("Decision", ["approved","rejected"])
    comments = st.text_input("Comments")
    if st.button("Submit Decision"):
        with engine.begin() as conn:
            conn.execute(approvals.insert().values(
                requisition_id=int(rid), approver_id=st.session_state.user["id"],
                decision=decision, comments=comments, decided_at=datetime.utcnow()
            ))
            conn.execute(requisitions.update().where(requisitions.c.id==int(rid)).values(
                status="approved" if decision=="approved" else "rejected"
            ))
        st.success(f"Requisition {rid} {decision}")

def page_rfqs_bids():
    st.header("RFQs & Bids")
    col1, col2 = st.columns(2)
    with col1:
        r_for_rfq = st.number_input("Requisition ID for RFQ", min_value=1, step=1)
        if st.button("Create RFQ from Requisition"):
            rfq_no = now_ts_id("RFQ")
            with engine.begin() as conn:
                rfq_id = conn.execute(rfqs.insert().values(
                    requisition_id=int(r_for_rfq), rfq_no=rfq_no, status="sent"
                )).inserted_primary_key[0]
                rows = conn.execute(select(requisition_items).where(requisition_items.c.requisition_id==int(r_for_rfq))).mappings().all()
                for r in rows:
                    conn.execute(rfq_items.insert().values(rfq_id=rfq_id, item_desc=r["description"], qty=r["qty"]))
                conn.execute(requisitions.update().where(requisitions.c.id==int(r_for_rfq)).values(status="sourced"))
            st.success(f"RFQ {rfq_no} created (ID {rfq_id})")
    with col2:
        rfq_id_for_bid = st.number_input("RFQ ID", min_value=1, step=1)
        supplier_id = st.number_input("Supplier ID", min_value=1, step=1)
        price = st.number_input("Total Bid Price", min_value=0.0, step=0.01)
        ltd = st.number_input("Lead time (days)", min_value=0, step=1)
        notes = st.text_input("Bid notes")
        if st.button("Record Bid"):
            with engine.begin() as conn:
                conn.execute(bids.insert().values(
                    rfq_id=int(rfq_id_for_bid), supplier_id=int(supplier_id),
                    price=price, lead_time_days=int(ltd), notes=notes
                ))
            st.success("Bid recorded")
    with engine.begin() as conn:
        df_rfq = pd.read_sql_query(sqltext("SELECT id, rfq_no, requisition_id, status FROM rfqs ORDER BY id DESC LIMIT 200"), conn)
        df_bids = pd.read_sql_query(sqltext("""
            SELECT b.id, b.rfq_id, s.name supplier, b.price, b.lead_time_days
            FROM bids b JOIN suppliers s ON s.id=b.supplier_id
            ORDER BY b.rfq_id DESC, b.price ASC
        """), conn)
    st.subheader("RFQs")
    st.dataframe(df_rfq, use_container_width=True)
    st.subheader("Bids (best price first)")
    st.dataframe(df_bids, use_container_width=True)

def page_pos():
    st.header("Purchase Orders")
    rfq_id_sel = st.number_input("RFQ ID (winner)", min_value=1, step=1)
    supplier_id_sel = st.number_input("Supplier ID (winner)", min_value=1, step=1)
    if st.button("Create PO"):
        with engine.begin() as conn:
            po_no = now_ts_id("PO")
            po_id = conn.execute(purchase_orders.insert().values(
                po_no=po_no, supplier_id=int(supplier_id_sel), rfq_id=int(rfq_id_sel), status="created"
            )).inserted_primary_key[0]
            items_rows = conn.execute(select(rfq_items).where(rfq_items.c.rfq_id==int(rfq_id_sel))).mappings().all()
            bid = conn.execute(
                select(bids.c.price).where(and_(bids.c.rfq_id==int(rfq_id_sel), bids.c.supplier_id==int(supplier_id_sel)))
                .order_by(bids.c.id.desc())
            ).scalar()
            total_qty = sum([float(r["qty"]) for r in items_rows]) or 1.0
            unit_price = float(bid)/total_qty if bid is not None else 0
            for r in items_rows:
                conn.execute(po_items.insert().values(po_id=po_id, item_desc=r["item_desc"], qty=r["qty"], unit_price=unit_price))
        st.success(f"PO {po_no} created (ID {po_id})")
    with engine.begin() as conn:
        df = pd.read_sql_query(sqltext("""
            SELECT po.id, po.po_no, s.name supplier, po.status,
                   (SELECT SUM(qty*unit_price) FROM po_items WHERE po_id=po.id) AS total
            FROM purchase_orders po JOIN suppliers s ON s.id=po.supplier_id
            ORDER BY po.id DESC LIMIT 200
        """), conn)
    st.dataframe(df, use_container_width=True)

def page_grn_insp():
    st.header("Goods Receipt & Inspection (Auto-Inventory IN)")
    with engine.begin() as conn:
        wh_opts = pd.read_sql_query(sqltext("SELECT id, name FROM warehouses ORDER BY name"), conn)
    if wh_opts.empty:
        st.warning("Create a warehouse first (see '8) Warehouses').")
        wh_id = 0
    else:
        wh_id = st.selectbox("Warehouse to receive into", wh_opts["id"].tolist(),
                             format_func=lambda x: wh_opts.set_index("id").loc[x, "name"])

    col1, col2 = st.columns(2)
    with col1:
        po_id_in = st.number_input("PO ID", min_value=1, step=1)
        received_by = st.number_input("Receiver user ID", min_value=1, step=1, value=st.session_state.user["id"])
        if st.button("Record Goods Receipt"):
            if wh_id == 0:
                st.error("Please create/select a warehouse first.")
                return
            with engine.begin() as conn:
                grn_no = now_ts_id("GRN")
                grn_id = conn.execute(goods_receipts.insert().values(
                    po_id=int(po_id_in), grn_no=grn_no, received_at=date.today(), received_by=int(received_by)
                )).inserted_primary_key[0]
                conn.execute(purchase_orders.update().where(purchase_orders.c.id==int(po_id_in)).values(status="delivered"))
                po_lines = conn.execute(select(po_items).where(po_items.c.po_id==int(po_id_in))).mappings().all()
                for line in po_lines:
                    item_name = str(line["item_desc"]).strip()
                    it = conn.execute(select(items).where(items.c.name==item_name)).mappings().first()
                    if not it:
                        iid = conn.execute(items.insert().values(name=item_name, uom="ea", last_price=line["unit_price"])).inserted_primary_key[0]
                        item_id = iid
                    else:
                        item_id = it["id"]
                        conn.execute(items.update().where(items.c.id==item_id).values(last_price=line["unit_price"]))
                    inventory_in(conn, int(wh_id), int(item_id), float(line["qty"]), "GRN", int(grn_id))
            st.success(f"GRN {grn_no} recorded (ID {grn_id}) — Inventory updated ✅")

    with col2:
        grn_id_in = st.number_input("GRN ID", min_value=1, step=1)
        result = st.selectbox("Inspection Result", ["accepted","rejected","accepted_with_notes"])
        notes = st.text_input("Inspection notes")
        if st.button("Save Inspection"):
            with engine.begin() as conn:
                conn.execute(inspections.insert().values(
                    grn_id=int(grn_id_in), result=result, notes=notes, inspected_by=st.session_state.user["id"]
                ))
            st.success("Inspection saved")

    with engine.begin() as conn:
        df = pd.read_sql_query(sqltext("SELECT id, grn_no, po_id, received_at FROM goods_receipts ORDER BY id DESC LIMIT 200"), conn)
    st.subheader("Recent GRNs")
    st.dataframe(df, use_container_width=True)

def page_invoices():
    st.header("Invoices, Payments & Three-way Match")
    with st.expander("Record Invoice"):
        col1, col2 = st.columns(2)
        with col1:
            po_id_inv = st.number_input("PO ID", min_value=1, step=1, key="po_inv")
            supplier_id_inv = st.number_input("Supplier ID", min_value=1, step=1, key="sup_inv")
            invoice_no = st.text_input("Invoice No")
            amount_inv = st.number_input("Invoice Amount (header)", min_value=0.0, step=0.01)
        with col2:
            st.caption("Add invoice lines (match PO items):")
            inv_line_desc = st.text_input("Item desc")
            inv_line_qty = st.number_input("Qty", min_value=0.0, step=1.0)
            inv_line_unit = st.number_input("Unit price", min_value=0.0, step=0.01)

        if st.button("Save Invoice Header"):
            with engine.begin() as conn:
                inv_id = conn.execute(invoices.insert().values(
                    supplier_id=int(supplier_id_inv), po_id=int(po_id_inv), invoice_no=invoice_no,
                    amount=amount_inv, status="received", received_at=date.today()
                )).inserted_primary_key[0]
            st.session_state["current_invoice_id"] = inv_id
            st.success(f"Invoice {invoice_no} saved (ID {inv_id})")

        if st.button("Add Invoice Line"):
            inv_id = st.session_state.get("current_invoice_id")
            if not inv_id:
                st.warning("Save invoice header first.")
            else:
                with engine.begin() as conn:
                    conn.execute(invoice_items.insert().values(
                        invoice_id=int(inv_id), item_desc=inv_line_desc, qty=inv_line_qty, unit_price=inv_line_unit
                    ))
                st.success("Invoice line added")

    with st.expander("Mark Payment"):
        inv_id_pay = st.number_input("Invoice ID", min_value=1, step=1)
        method = st.text_input("Method", value="bank_transfer")
        ref = st.text_input("Reference")
        if st.button("Mark as Paid"):
            with engine.begin() as conn:
                conn.execute(invoices.update().where(invoices.c.id==int(inv_id_pay)).values(status="paid"))
                conn.execute(payments.insert().values(invoice_id=int(inv_id_pay), paid_at=date.today(), method=method, reference=ref))
            st.success("Payment recorded")

    st.subheader("Three-way Match Checker")
    inv_id_chk = st.number_input("Invoice ID to validate", min_value=1, step=1)
    if st.button("Run 3-way match"):
        with engine.begin() as conn:
            inv = conn.execute(select(invoices).where(invoices.c.id==int(inv_id_chk))).mappings().first()
            if not inv:
                st.error("Invoice not found")
                return
            po_id = inv["po_id"]
            po_lines = conn.execute(select(po_items).where(po_items.c.po_id==po_id)).mappings().all()
            po_qty = sum([float(r["qty"]) for r in po_lines])
            po_total = sum([float(r["qty"])*float(r["unit_price"]) for r in po_lines])
            grn_rows = conn.execute(select(goods_receipts).where(goods_receipts.c.po_id==po_id)).mappings().all()
            inv_lines = conn.execute(select(invoice_items).where(invoice_items.c.invoice_id==inv_id_chk)).mappings().all()
            inv_qty = sum([float(r["qty"]) for r in inv_lines])
            inv_total = sum([float(r["qty"])*float(r["unit_price"]) for r in inv_lines])

        match_qty = "OK" if abs(inv_qty - po_qty) < 1e-6 else "MISMATCH"
        match_amt = "OK" if abs(inv_total - po_total) < 1e-2 else "MISMATCH"
        st.write(f"PO Qty: **{po_qty}** | Invoice Qty: **{inv_qty}** → {match_qty}")
        st.write(f"PO Amount: **{po_total:.2f}** | Invoice Amount (lines): **{inv_total:.2f}** → {match_amt}")

        if not grn_rows:
            st.warning("No GRN found — goods not received")
        if match_qty == "OK" and match_amt == "OK" and grn_rows:
            with engine.begin() as conn:
                conn.execute(invoices.update().where(invoices.c.id==int(inv_id_chk)).values(status="matched"))
            st.success("Invoice marked as MATCHED ✅")
        else:
            st.error("Three-way match failed ❌")

    with engine.begin() as conn:
        df = pd.read_sql_query(sqltext("SELECT id, invoice_no, po_id, amount, status FROM invoices ORDER BY id DESC LIMIT 200"), conn)
    st.subheader("Invoices")
    st.dataframe(df, use_container_width=True)

def page_suppliers():
    st.header("Suppliers & Scores")
    with st.form("add_supplier", clear_on_submit=True):
        name = st.text_input("Supplier name")
        contact = st.text_input("Contact person")
        email = st.text_input("Email")
        phone = st.text_input("Phone")
        submitted = st.form_submit_button("Add Supplier")
    if submitted:
        with engine.begin() as conn:
            sid = conn.execute(suppliers.insert().values(name=name, contact=contact, email=email, phone=phone)).inserted_primary_key[0]
        st.success(f"Supplier added (ID {sid})")

    with engine.begin() as conn:
        df = pd.read_sql_query(sqltext("SELECT id, name, status, rating FROM suppliers ORDER BY id DESC LIMIT 200"), conn)
    st.dataframe(df, use_container_width=True)

    st.subheader("Performance Scoring")
    supplier_id_s = st.number_input("Supplier ID", min_value=1, step=1)
    quality = st.slider("Quality", 0, 10, 8)
    delivery = st.slider("Delivery", 0, 10, 8)
    service = st.slider("Service", 0, 10, 8)
    notes = st.text_input("Notes", value="")
    if st.button("Save Score"):
        overall = round((quality+delivery+service)/3, 2)
        with engine.begin() as conn:
            conn.execute(supplier_scores.insert().values(
                supplier_id=int(supplier_id_s), score_date=date.today(),
                quality=quality, delivery=delivery, service=service, overall=overall, notes=notes
            ))
            conn.execute(suppliers.update().where(suppliers.c.id==int(supplier_id_s)).values(rating=overall))
        st.success("Score saved")

def page_warehouses():
    st.header("Warehouses")
    with st.form("add_wh", clear_on_submit=True):
        name = st.text_input("Warehouse name")
        code = st.text_input("Code", placeholder="MAIN-1")
        address = st.text_input("Address")
        sub = st.form_submit_button("Create Warehouse")
    if sub:
        with engine.begin() as conn:
            wid = conn.execute(warehouses.insert().values(name=name, code=code, address=address)).inserted_primary_key[0]
        st.success(f"Warehouse created (ID {wid})")

    st.subheader("All Warehouses")
    with engine.begin() as conn:
        df = pd.read_sql_query(sqltext("SELECT id, name, code, address, created_at FROM warehouses ORDER BY id DESC"), conn)
    st.dataframe(df, use_container_width=True)

    st.subheader("Bin Locations")
    wh_id = st.number_input("Warehouse ID (for bin)", min_value=1, step=1)
    bin_code = st.text_input("Bin code (e.g., A-01-03)")
    bin_desc = st.text_input("Description")
    if st.button("Add Bin"):
        with engine.begin() as conn:
            conn.execute(bin_locations.insert().values(warehouse_id=int(wh_id), code=bin_code, desc=bin_desc))
        st.success("Bin added")

    with engine.begin() as conn:
        bins = pd.read_sql_query(sqltext("""
            SELECT b.id, w.name AS warehouse, b.code, b.desc
            FROM bin_locations b JOIN warehouses w ON w.id=b.warehouse_id
            ORDER BY b.id DESC LIMIT 300
        """), conn)
    st.dataframe(bins, use_container_width=True)

def page_inventory():
    st.header("Inventory & Transfers")
    with engine.begin() as conn:
        inv = pd.read_sql_query(sqltext("""
            SELECT i.id, w.name AS warehouse, it.name AS item, i.qty, i.updated_at
            FROM inventory i
            JOIN warehouses w ON w.id=i.warehouse_id
            JOIN items it ON it.id=i.item_id
            ORDER BY w.name, it.name
        """), conn)
    st.subheader("On-hand Inventory")
    st.dataframe(inv, use_container_width=True)

    st.subheader("Manual Transfer")
    col1, col2, col3 = st.columns(3)
    with engine.begin() as conn:
        whs = pd.read_sql_query(sqltext("SELECT id, name FROM warehouses ORDER BY name"), conn)
        its = pd.read_sql_query(sqltext("SELECT id, name FROM items ORDER BY name LIMIT 1000"), conn)

    if whs.empty or its.empty:
        st.info("Create at least one warehouse and one item (items get created during GRN) to use transfers.")
        return

    wh_from = col1.selectbox("From Warehouse", whs["id"].tolist(), format_func=lambda x: whs.set_index("id").loc[x,"name"])
    wh_to = col2.selectbox("To Warehouse", whs["id"].tolist(), format_func=lambda x: whs.set_index("id").loc[x,"name"])
    item_sel = col3.selectbox("Item", its["id"].tolist(), format_func=lambda x: its.set_index("id").loc[x,"name"])

    qty = st.number_input("Qty to transfer", min_value=0.0, step=1.0)
    if st.button("Transfer"):
        try:
            with engine.begin() as conn:
                transfer_stock(conn, int(wh_from), int(wh_to), int(item_sel), float(qty))
            st.success("Transfer completed")
        except Exception as e:
            st.error(str(e))

    st.subheader("Recent Stock Movements")
    with engine.begin() as conn:
        mv = pd.read_sql_query(sqltext("""
            SELECT m.id, m.movement_no, m.movement_type, w1.name AS wh_from, w2.name AS wh_to,
                   it.name AS item, m.qty, m.reason, m.ref_entity, m.ref_id, m.created_at
            FROM stock_movements m
            LEFT JOIN warehouses w1 ON w1.id=m.warehouse_id
            LEFT JOIN warehouses w2 ON w2.id=m.warehouse_to_id
            LEFT JOIN items it ON it.id=m.item_id
            ORDER BY m.id DESC LIMIT 300
        """), conn)
    st.dataframe(mv, use_container_width=True)

def page_deliveries():
    st.header("Outbound Deliveries (Auto-Inventory OUT)")
    with engine.begin() as conn:
        whs = pd.read_sql_query(sqltext("SELECT id, name FROM warehouses ORDER BY name"), conn)
        its = pd.read_sql_query(sqltext("SELECT id, name FROM items ORDER BY name LIMIT 1000"), conn)

    if whs.empty:
        st.warning("Create a warehouse first.")
        return

    wh_id = st.selectbox("Warehouse", whs["id"].tolist(), format_func=lambda x: whs.set_index("id").loc[x,"name"])
    customer = st.text_input("Customer name")
    sched = st.date_input("Scheduled date", date.today())

    if st.button("Create Delivery Order"):
        with engine.begin() as conn:
            do_no = now_ts_id("DO")
            did = conn.execute(deliveries.insert().values(
                do_no=do_no, warehouse_id=int(wh_id), customer_name=customer,
                status="created", scheduled_date=sched
            )).inserted_primary_key[0]
        st.session_state["current_do"] = did
        st.success(f"Delivery created {do_no} (ID {did})")

    st.subheader("Add Delivery Lines")
    do_cur = st.session_state.get("current_do")
    if do_cur:
        if its.empty:
            st.info("No items exist yet. Create items via GRN first.")
        else:
            item_sel = st.selectbox("Item", its["id"].tolist(), format_func=lambda x: its.set_index("id").loc[x,"name"])
            qty = st.number_input("Qty to ship", min_value=0.0, step=1.0)
            if st.button("Add Line"):
                with engine.begin() as conn:
                    conn.execute(delivery_items.insert().values(delivery_id=int(do_cur), item_id=int(item_sel), qty=float(qty)))
                st.success("Line added")

            if st.button("Ship (auto deduct inventory)"):
                try:
                    with engine.begin() as conn:
                        lines = conn.execute(select(delivery_items).where(delivery_items.c.delivery_id==int(do_cur))).mappings().all()
                        for ln in lines:
                            inventory_out(conn, int(wh_id), int(ln["item_id"]), float(ln["qty"]), "DO", int(do_cur))
                        conn.execute(deliveries.update().where(deliveries.c.id==int(do_cur)).values(status="shipped"))
                    st.success("Delivery shipped — Inventory deducted ✅")
                except Exception as e:
                    st.error(str(e))

    with engine.begin() as conn:
        d_head = pd.read_sql_query(sqltext("""
            SELECT d.id, d.do_no, w.name warehouse, d.customer_name, d.status, d.scheduled_date, d.created_at
            FROM deliveries d JOIN warehouses w ON w.id=d.warehouse_id
            ORDER BY d.id DESC LIMIT 200
        """), conn)
    st.subheader("Recent Deliveries")
    st.dataframe(d_head, use_container_width=True)

def page_fleet():
    st.header("Vehicles & Drivers")
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Vehicles")
        plate = st.text_input("Plate No")
        cap = st.number_input("Capacity (kg)", min_value=0.0, step=10.0)
        if st.button("Add Vehicle"):
            with engine.begin() as conn:
                conn.execute(vehicles.insert().values(plate_no=plate, capacity_kg=cap, active=True))
            st.success("Vehicle added")
    with col2:
        st.subheader("Drivers")
        dname = st.text_input("Driver Name")
        dphone = st.text_input("Phone")
        if st.button("Add Driver"):
            with engine.begin() as conn:
                conn.execute(drivers.insert().values(name=dname, phone=dphone, active=True))
            st.success("Driver added")

    with engine.begin() as conn:
        v = pd.read_sql_query(sqltext("SELECT id, plate_no, capacity_kg, active FROM vehicles ORDER BY id DESC"), conn)
        d = pd.read_sql_query(sqltext("SELECT id, name, phone, active FROM drivers ORDER BY id DESC"), conn)
    st.subheader("Vehicles")
    st.dataframe(v, use_container_width=True)
    st.subheader("Drivers")
    st.dataframe(d, use_container_width=True)

    st.subheader("Assign Vehicle & Driver to Delivery")
    with engine.begin() as conn:
        dels = pd.read_sql_query(sqltext("SELECT id, do_no FROM deliveries ORDER BY id DESC LIMIT 300"), conn)
        vs = pd.read_sql_query(sqltext("SELECT id, plate_no FROM vehicles WHERE active=1 ORDER BY id DESC"), conn)
        drs = pd.read_sql_query(sqltext("SELECT id, name FROM drivers WHERE active=1 ORDER BY id DESC"), conn)

    if dels.empty or vs.empty or drs.empty:
        st.info("Create at least one delivery, vehicle, and driver to assign.")
        return

    del_sel = st.selectbox("Delivery", dels["id"].tolist(), format_func=lambda x: dels.set_index("id").loc[x,"do_no"])
    v_sel = st.selectbox("Vehicle", vs["id"].tolist(), format_func=lambda x: vs.set_index("id").loc[x,"plate_no"])
    d_sel = st.selectbox("Driver", drs["id"].tolist(), format_func=lambda x: drs.set_index("id").loc[x,"name"])

    if st.button("Assign"):
        with engine.begin() as conn:
            conn.execute(delivery_assignments.insert().values(delivery_id=int(del_sel), vehicle_id=int(v_sel), driver_id=int(d_sel)))
        st.success("Assigned")

def page_documents():
    st.header("Documents")
    UPLOAD_DIR = os.path.abspath("./uploads")
    os.makedirs(UPLOAD_DIR, exist_ok=True)

    def save_local(uploaded_file) -> str:
        path = os.path.join(UPLOAD_DIR, uploaded_file.name)
        with open(path, "wb") as f:
            f.write(uploaded_file.getbuffer())
        return path

    entity_type = st.selectbox("Entity Type", ["REQ","RFQ","PO","GRN","INV","SUPPLIER","DELIVERY"])
    entity_id = st.number_input("Entity ID", min_value=1, step=1)
    file = st.file_uploader("Select file")

    if st.button("Upload") and file:
        path = save_local(file)
        with engine.begin() as conn:
            conn.execute(documents.insert().values(entity_type=entity_type, entity_id=int(entity_id),
                                                 filename=file.name, path=path))
        st.success(f"Uploaded to {path}")

    with engine.begin() as conn:
        df = pd.read_sql_query(sqltext("""
            SELECT id, entity_type, entity_id, filename, path, uploaded_at
            FROM documents ORDER BY id DESC LIMIT 200
        """), conn)
    st.subheader("Recent Documents")
    st.dataframe(df, use_container_width=True)

def page_reports():
    st.header("Reports & Analytics")

    st.subheader("Spend by Supplier")
    with engine.begin() as conn:
        spend = pd.read_sql_query(sqltext("""
            SELECT s.name supplier, COALESCE(SUM(i.amount),0) AS spend
            FROM suppliers s
            LEFT JOIN invoices i ON i.supplier_id=s.id
            GROUP BY s.name ORDER BY spend DESC
        """), conn)
    if not spend.empty:
        st.bar_chart(spend.set_index("supplier"))
    st.dataframe(spend, use_container_width=True)

    st.subheader("Lead-time: Requisition to Goods Receipt")
    if USING_MYSQL:
        lead_sql = """
            SELECT r.req_no, po.po_no, gr.grn_no, DATEDIFF(gr.received_at, r.created_at) AS days
            FROM requisitions r
            JOIN rfqs rf ON rf.requisition_id=r.id
            JOIN purchase_orders po ON po.rfq_id=rf.id
            JOIN goods_receipts gr ON gr.po_id=po.id
            ORDER BY r.id DESC LIMIT 300
        """
    else:
        lead_sql = """
            SELECT r.req_no, po.po_no, gr.grn_no,
                   CAST((julianday(gr.received_at) - julianday(date(r.created_at))) AS INTEGER) AS days
            FROM requisitions r
            JOIN rfqs rf ON rf.requisition_id=r.id
            JOIN purchase_orders po ON po.rfq_id=rf.id
            JOIN goods_receipts gr ON gr.po_id=po.id
            ORDER BY r.id DESC LIMIT 300
        """
    with engine.begin() as conn:
        lead = pd.read_sql_query(sqltext(lead_sql), conn)
    st.dataframe(lead, use_container_width=True)

    st.subheader("OTIF (On-Time In-Full)")
    with engine.begin() as conn:
        otif = pd.read_sql_query(sqltext("""
            SELECT r.req_no, r.needed_by, gr.received_at,
                   CASE WHEN gr.received_at IS NOT NULL AND r.needed_by IS NOT NULL AND gr.received_at <= r.needed_by THEN 1 ELSE 0 END AS on_time,
                   (SELECT COALESCE(SUM(qty),0) FROM po_items pi WHERE pi.po_id=po.id) AS ordered_qty,
                   (SELECT COALESCE(SUM(ii.qty),0) FROM invoices inv
                    JOIN invoice_items ii ON ii.invoice_id=inv.id
                    WHERE inv.po_id=po.id) AS invoiced_qty
            FROM requisitions r
            JOIN rfqs rf ON rf.requisition_id=r.id
            JOIN purchase_orders po ON po.rfq_id=rf.id
            LEFT JOIN goods_receipts gr ON gr.po_id=po.id
            ORDER BY r.id DESC LIMIT 300
        """), conn)

    if not otif.empty:
        otif["in_full"] = (otif["invoiced_qty"] >= otif["ordered_qty"]).astype(int)
        otif["otif"] = (otif["on_time"].astype(int) & otif["in_full"].astype(int)).astype(int)
        st.metric("OTIF % (sample)", f"{(otif['otif'].mean() * 100):.1f}%")
        st.dataframe(otif[["req_no","needed_by","received_at","on_time","ordered_qty","invoiced_qty","in_full","otif"]],
                     use_container_width=True)
    else:
        st.info("No data for OTIF yet.")

    st.subheader("Inventory Valuation")
    with engine.begin() as conn:
        val = pd.read_sql_query(sqltext("""
            SELECT w.name AS warehouse, it.name AS item, inv.qty, it.last_price,
                   (inv.qty * it.last_price) AS value
            FROM inventory inv
            JOIN items it ON it.id=inv.item_id
            JOIN warehouses w ON w.id=inv.warehouse_id
            ORDER BY w.name, it.name
        """), conn)
    if not val.empty:
        total_val = float(val["value"].fillna(0).sum())
        st.metric("Total Inventory Value", f"{total_val:,.2f}")
        st.dataframe(val, use_container_width=True)
    else:
        st.info("No inventory yet.")

def page_admin_users():
    st.header("Admin — Users")
    with st.form("add_user", clear_on_submit=True):
        name = st.text_input("Name")
        email = st.text_input("Email")
        pwd = st.text_input("Password", type="password")
        role = st.selectbox("Role", ["requester","approver","buyer","inspector","finance","admin"])
        dept = st.text_input("Department", value="Ops")
        submitted = st.form_submit_button("Create User")

    if submitted:
        try:
            with engine.begin() as conn:
                conn.execute(users.insert().values(
                    name=name, email=email, password_hash=sha256(pwd), role=role, dept=dept
                ))
            st.success("User created")
        except IntegrityError:
            st.error("Email already exists")

    with engine.begin() as conn:
        df = pd.read_sql_query(sqltext("SELECT id, name, email, role, dept, created_at FROM users ORDER BY id DESC LIMIT 200"), conn)
    st.dataframe(df, use_container_width=True)

# ---------------------------
# ROUTER
# ---------------------------
if choice.startswith("1)"):
    page_requisitions()
elif choice.startswith("2)"):
    page_approvals()
elif choice.startswith("3)"):
    page_rfqs_bids()
elif choice.startswith("4)"):
    page_pos()
elif choice.startswith("5)"):
    page_grn_insp()
elif choice.startswith("6)"):
    page_invoices()
elif choice.startswith("7)"):
    page_suppliers()
elif choice.startswith("8)"):
    page_warehouses()
elif choice.startswith("9)"):
    page_inventory()
elif choice.startswith("10)"):
    page_deliveries()
elif choice.startswith("11)"):
    page_fleet()
elif choice.startswith("12)"):
    page_documents()
elif choice.startswith("13)"):
    page_reports()
elif choice.startswith("Admin"):
    page_admin_users()

st.caption("""
✔️ Uses SQLite automatically when DB_HOST is not configured (Streamlit Cloud demo mode).
✔️ Demo user is available via sidebar login (recruiters can assess the app).
✔️ Auto-updates inventory on GRN; deducts on shipments.
✔️ Includes inventory valuation report.
""")