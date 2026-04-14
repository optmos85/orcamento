"""
╔══════════════════════════════════════════════════════════════╗
║         ORÇAMENTO MENSAL — Backend FastAPI v5.0              ║
║  PostgreSQL + E-mail + Multi-usuário                         ║
║                                                              ║
║  Variáveis de ambiente (configurar no Render):               ║
║    DATABASE_URL   — URL do PostgreSQL (Render fornece)       ║
║    GMAIL_USER     — seu e-mail Gmail                         ║
║    GMAIL_APP_PASS — senha de app do Gmail (não a normal)     ║
║    APP_URL        — URL pública do sistema no Render         ║
╚══════════════════════════════════════════════════════════════╝
"""
from __future__ import annotations
import json, os, io, re, hashlib, secrets, shutil, calendar as cal_mod, smtplib
from pathlib import Path
from typing import Any, Optional
from datetime import datetime, date, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

import chardet, uvicorn
try:
    import requests as req_lib; REQUESTS_OK = True
except ImportError:
    REQUESTS_OK = False

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor; DB_OK = True
except ImportError:
    DB_OK = False

from fastapi import FastAPI, HTTPException, UploadFile, File, Query, Cookie, Response, Depends
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# ══════════════════════ CONFIG ════════════════════════════════
STATIC_FILE  = Path(__file__).parent / "index.html"
BACKUP_DIR   = Path("backups"); BACKUP_DIR.mkdir(exist_ok=True)
DATABASE_URL = os.environ.get("DATABASE_URL", "")
GMAIL_USER   = os.environ.get("GMAIL_USER", "")
# Debug: mostrar se variáveis chegaram
print(f"[ENV] DATABASE_URL set: {bool(DATABASE_URL)} len={len(DATABASE_URL)}")
print(f"[ENV] DATABASE_URL starts: {DATABASE_URL[:30] if DATABASE_URL else 'EMPTY'}")
GMAIL_PASS   = os.environ.get("GMAIL_APP_PASS", "")
APP_URL      = os.environ.get("APP_URL", "http://localhost:8000")

app = FastAPI(title="Orçamento Mensal", version="5.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"],
                   allow_headers=["*"], allow_credentials=True)
_sessions: dict[str, str] = {}

# ══════════════════════ BANCO ════════════════════════════════
def get_conn():
    if not DB_OK: raise HTTPException(500, "psycopg2 não instalado")
    if not DATABASE_URL: raise HTTPException(500, "DATABASE_URL não configurada")
    url = DATABASE_URL.replace("postgres://", "postgresql://", 1)
    # Supabase Session Pooler requer sslmode
    if "sslmode" not in url:
        sep = "&" if "?" in url else "?"
        url = url + sep + "sslmode=require"
    return psycopg2.connect(url, cursor_factory=RealDictCursor,
                            connect_timeout=10)

def init_db():
    if not DB_OK or not DATABASE_URL:
        print("⚠ Sem banco — usando JSON local"); return
    try:
        conn = get_conn()
        print(f'✅ DB conectado: {conn.dsn[:50]}')
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id TEXT PRIMARY KEY, name TEXT NOT NULL,
                    email TEXT, salt TEXT NOT NULL, password_hash TEXT NOT NULL,
                    role TEXT DEFAULT 'user', color TEXT DEFAULT '#2ecc85',
                    must_change_password BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMPTZ DEFAULT NOW());
                CREATE TABLE IF NOT EXISTS user_data (
                    user_id TEXT, month_key TEXT, data JSONB DEFAULT '{}',
                    updated_at TIMESTAMPTZ DEFAULT NOW(),
                    PRIMARY KEY (user_id, month_key));
                CREATE TABLE IF NOT EXISTS global_metas (
                    user_id TEXT PRIMARY KEY, metas JSONB DEFAULT '[]',
                    updated_at TIMESTAMPTZ DEFAULT NOW());
                CREATE TABLE IF NOT EXISTS reset_tokens (
                    token TEXT PRIMARY KEY, user_id TEXT NOT NULL,
                    expires_at TIMESTAMPTZ NOT NULL, used BOOLEAN DEFAULT FALSE);
                CREATE TABLE IF NOT EXISTS pluggy_config (
                    user_id TEXT PRIMARY KEY, client_id TEXT,
                    client_secret TEXT, items JSONB DEFAULT '[]');
                """)
                conn.commit()
        print("✅ DB OK")
    except Exception as e:
        import traceback
        print(f"⚠ DB init error TYPE: {type(e).__name__}")
        print(f"⚠ DB init error MSG: {e}")
        print(traceback.format_exc())

@app.on_event("startup")
async def startup():
    init_db()
    _migrate_json()

def _migrate_json():
    """Migra users.json e data_*.json legados para o PostgreSQL."""
    if not DB_OK or not DATABASE_URL: return
    uf = Path("users.json")
    if not uf.exists(): return
    try:
        ud = json.loads(uf.read_text("utf-8"))
        with get_conn() as conn:
            with conn.cursor() as cur:
                for u in ud.get("users", []):
                    cur.execute("INSERT INTO users (id,name,email,salt,password_hash,role,color,must_change_password) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (id) DO NOTHING",
                        (u["id"],u["name"],u.get("email",""),u["salt"],u["password_hash"],u.get("role","user"),u.get("color","#2ecc85"),False))
                    df = Path(f"data_{u['id']}.json")
                    if df.exists():
                        raw = json.loads(df.read_text("utf-8"))
                        for k, v in raw.items():
                            if k == "_global_metas":
                                cur.execute("INSERT INTO global_metas (user_id,metas) VALUES (%s,%s) ON CONFLICT (user_id) DO NOTHING", (u["id"], json.dumps(v)))
                            elif not k.startswith("_"):
                                cur.execute("INSERT INTO user_data (user_id,month_key,data) VALUES (%s,%s,%s) ON CONFLICT (user_id,month_key) DO NOTHING", (u["id"],k,json.dumps(v)))
                conn.commit()
        print("✅ Migração JSON→DB concluída")
    except Exception as e:
        print(f"⚠ Migração: {e}")

# ══════════════════════ USER HELPERS ═════════════════════════
def db_get_user(uid: str):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM users WHERE id=%s", (uid,))
            r = cur.fetchone(); return dict(r) if r else None

def db_get_user_by_email(email: str):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM users WHERE LOWER(email)=LOWER(%s)", (email,))
            r = cur.fetchone(); return dict(r) if r else None

def db_list_users():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id,name,email,role,color,must_change_password,created_at FROM users ORDER BY created_at")
            return [dict(r) for r in cur.fetchall()]

def db_save_user(u: dict):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO users (id,name,email,salt,password_hash,role,color,must_change_password) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (id) DO UPDATE SET name=EXCLUDED.name,email=EXCLUDED.email,salt=EXCLUDED.salt,password_hash=EXCLUDED.password_hash,role=EXCLUDED.role,color=EXCLUDED.color,must_change_password=EXCLUDED.must_change_password",
                (u["id"],u["name"],u.get("email",""),u["salt"],u["password_hash"],u.get("role","user"),u.get("color","#2ecc85"),u.get("must_change_password",False)))
            conn.commit()

def db_delete_user(uid: str):
    with get_conn() as conn:
        with conn.cursor() as cur:
            for tbl in ["users","user_data","global_metas","pluggy_config"]:
                cur.execute(f"DELETE FROM {tbl} WHERE user_id=%s" if tbl!="users" else "DELETE FROM users WHERE id=%s", (uid,))
            conn.commit()

# ══════════════════════ DATA HELPERS ═════════════════════════
def db_load_month(uid: str, key: str) -> dict:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT data FROM user_data WHERE user_id=%s AND month_key=%s", (uid,key))
            r = cur.fetchone(); return dict(r["data"]) if r else {}

def db_save_month(uid: str, key: str, data: dict):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO user_data (user_id,month_key,data,updated_at) VALUES (%s,%s,%s,NOW()) ON CONFLICT (user_id,month_key) DO UPDATE SET data=EXCLUDED.data,updated_at=NOW()",
                (uid, key, json.dumps(data)))
            conn.commit()
    _backup(uid, key, data)

def _backup(uid, key, data):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    dst = BACKUP_DIR / f"backup_{uid}_{key}_{ts}.json"
    dst.write_text(json.dumps(data, ensure_ascii=False, indent=2), "utf-8")
    baks = sorted(BACKUP_DIR.glob(f"backup_{uid}_*.json"))
    for old in baks[:-30]: old.unlink(missing_ok=True)

def db_list_months(uid: str) -> list:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT month_key, data FROM user_data WHERE user_id=%s ORDER BY month_key", (uid,))
            rows = cur.fetchall()
    result = []
    for r in rows:
        m = migrate(dict(r["data"]))
        tr = sum(x["valor"] for x in m.get("receitas",[]))
        td = sum(x["valor"] for x in m.get("despesas",[]))
        result.append({"key": r["month_key"], "receitas": tr, "despesas": td, "saldo": tr-td})
    return result

def db_get_global_metas(uid: str) -> list:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT metas FROM global_metas WHERE user_id=%s", (uid,))
            r = cur.fetchone(); return list(r["metas"]) if r else []

def db_save_global_metas(uid: str, metas: list):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO global_metas (user_id,metas,updated_at) VALUES (%s,%s,NOW()) ON CONFLICT (user_id) DO UPDATE SET metas=EXCLUDED.metas,updated_at=NOW()",
                (uid, json.dumps(metas)))
            conn.commit()

# ══════════════════════ E-MAIL ════════════════════════════════
def send_email(to: str, subject: str, html: str) -> bool:
    if not GMAIL_USER or not GMAIL_PASS:
        print(f"⚠ Email não configurado — para: {to} assunto: {subject}"); return False
    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject; msg["From"] = f"Orçamento <{GMAIL_USER}>"; msg["To"] = to
        msg.attach(MIMEText(html, "html", "utf-8"))
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as s:
            s.login(GMAIL_USER, GMAIL_PASS); s.sendmail(GMAIL_USER, to, msg.as_string())
        return True
    except Exception as e:
        print(f"⚠ Email error: {e}"); return False

def _reset_html(name, link):
    return f"""<div style="font-family:Arial,sans-serif;max-width:500px;margin:0 auto;padding:30px">
    <h2 style="color:#2ecc85">💰 Orçamento Mensal</h2>
    <p>Olá, <strong>{name}</strong>!</p>
    <p>Clique para redefinir sua senha:</p>
    <a href="{link}" style="display:inline-block;background:#2ecc85;color:#fff;padding:12px 28px;border-radius:8px;text-decoration:none;font-weight:bold;margin:16px 0">Redefinir Senha</a>
    <p style="color:#888;font-size:.85rem">Este link expira em <strong>1 hora</strong>. Se não solicitou, ignore.</p></div>"""

def _invite_html(name, inviter, link, temp_pass):
    return f"""<div style="font-family:Arial,sans-serif;max-width:500px;margin:0 auto;padding:30px">
    <h2 style="color:#2ecc85">💰 Orçamento Mensal</h2>
    <p>Olá, <strong>{name}</strong>!</p>
    <p><strong>{inviter}</strong> criou uma conta para você.</p>
    <a href="{link}" style="display:inline-block;background:#2ecc85;color:#fff;padding:12px 28px;border-radius:8px;text-decoration:none;font-weight:bold;margin:16px 0">Acessar Sistema</a>
    <p style="color:#666;font-size:.85rem">Usuário: <code>{name}</code><br>Senha temporária: <code style="background:#f0f0f0;padding:2px 6px;border-radius:4px">{temp_pass}</code><br>Você será obrigado a trocar a senha no primeiro acesso.</p></div>"""

# ══════════════════════ AUTH HELPERS ═════════════════════════
def hash_pw(pw, salt): return hashlib.sha256(f"{salt}{pw}".encode()).hexdigest()
def create_session(uid):
    t = secrets.token_urlsafe(32); _sessions[t] = uid; return t
def get_session_uid(session_token: Optional[str] = Cookie(default=None)):
    return _sessions.get(session_token)
def require_auth(session_token: Optional[str] = Cookie(default=None)) -> str:
    uid = get_session_uid(session_token)
    if not uid: raise HTTPException(401, "Não autenticado")
    return uid
def require_admin(uid: str = Depends(require_auth)) -> str:
    u = db_get_user(uid)
    if not u or u.get("role") != "admin": raise HTTPException(403, "Apenas admin")
    return uid

# ══════════════════════ DADOS PADRÃO ═════════════════════════
DESPESAS_PADRAO = [
    ("Cartão de Crédito Nubank","Essencial"),("Alimentação","Essencial"),
    ("Seguro Carro","Essencial"),("Gasolina","Essencial"),
    ("Provisão IPVA","Essencial"),("Conta celular","Essencial"),
    ("Curso","Essencial"),("Cartão de Crédito Inter","Essencial"),
    ("Cartão de Crédito Bradesco","Essencial"),("IPVA Parcelado","Essencial"),
    ("Notebook","Essencial"),("Consórcio","Poupança"),("Poupança","Poupança"),
    ("Assinatura Globo Play","Desejo"),("Netflix","Desejo"),
    ("Assinatura Programa X","Desejo"),("Lazer / Passeio","Desejo"),("Aquisição Pessoal","Desejo"),
]
INTER_CATS = {
    "Alimentação":["supermercado","mercado","ifood","rappi","restaurante","lanchonete","padaria","hortifruti","carrefour","atacadão","assai"],
    "Gasolina":["posto","shell","ipiranga","petrobras","raizen","gasolina","etanol","combustivel"],
    "Conta celular":["tim","claro","vivo","nextel","celular"],
    "Netflix":["netflix"],"Assinatura Globo Play":["globoplay","globo play"],
    "Lazer / Passeio":["cinema","teatro","show","ingresso","spotify","deezer","disney","hbo","apple tv"],
    "Poupança":["poupança","poupanca","investimento","cdb","tesouro","renda fixa"],
    "Cartão de Crédito Nubank":["nubank","nu pagamentos"],
    "Cartão de Crédito Inter":["inter pagamento","banco inter"],
    "Cartão de Crédito Bradesco":["bradesco"],
    "Curso":["udemy","coursera","alura","escola","faculdade","mensalidade"],
    "Seguro Carro":["seguro auto","seguro carro","porto seguro","azul seguro"],
    "Aquisição Pessoal":["amazon","shopee","mercado livre","americanas","submarino","magazine"],
}
def guess_cat(desc):
    low = desc.lower()
    for cat, kws in INTER_CATS.items():
        if any(k in low for k in kws): return cat
    return "Outros"

def brl(v): return f"R$ {v:,.2f}".replace(",","X").replace(".",",").replace("X",".")

def clean_val(s):
    s = str(s or "0").strip().replace("R$","").replace("+","").replace(" ","")
    neg = s.startswith("-"); s = s.lstrip("-")
    if re.search(r"\d\.\d{3},\d{2}", s): s = s.replace(".","").replace(",",".")
    else: s = s.replace(",",".")
    try: v = float(s); return -v if neg else v
    except: return 0.0

def default_month():
    return {
        "receitas":[{"nome":"Salário","valor":0.0,"data_credito":""},{"nome":"Extra","valor":0.0,"data_credito":""}],
        "despesas":[{"nome":n,"categoria":c,"prestacao":"","valor":0.0,"vencimento":"","situacao":"","data_pgto":"","obs":"","ordem":i,"recorrente":False} for i,(n,c) in enumerate(DESPESAS_PADRAO)],
        "poupanca":{"saldo_anterior":0.0,"aporte":0.0,"juros":0.0},
        "compras":[],"metas":[],
    }

def migrate(m):
    if isinstance(m.get("receitas"), dict):
        old = m["receitas"]
        m["receitas"] = [{"nome":"Salário","valor":float(old.get("salario",0)),"data_credito":""},{"nome":"Extra","valor":float(old.get("extra",0)),"data_credito":""}]
    for i,d in enumerate(m.get("despesas",[])):
        if "categoria"  not in d: d["categoria"]  = "Essencial"
        if "ordem"      not in d: d["ordem"]       = i
        if "recorrente" not in d: d["recorrente"]  = False
    if "metas"   not in m: m["metas"]   = []
    if "compras" not in m: m["compras"] = []
    return m

def ensure_month(uid, key):
    m = db_load_month(uid, key)
    if not m:
        prev = [r["key"] for r in db_list_months(uid) if r["key"] < key]
        if prev:
            pm = migrate(db_load_month(uid, prev[-1]))
            m  = default_month()
            m["despesas"] = [{**d,"valor":d["valor"] if d.get("recorrente") else 0.0,"situacao":"","data_pgto":"","obs":""} for d in pm["despesas"]]
        else:
            m = default_month()
        db_save_month(uid, key, m)
    return migrate(m)

def month_totals(m):
    tr  = sum(r["valor"] for r in m.get("receitas",[]))
    td  = sum(d["valor"] for d in m.get("despesas",[]))
    ess = sum(d["valor"] for d in m.get("despesas",[]) if d.get("categoria")=="Essencial")
    pou = sum(d["valor"] for d in m.get("despesas",[]) if d.get("categoria")=="Poupança")
    des = sum(d["valor"] for d in m.get("despesas",[]) if d.get("categoria")=="Desejo")
    return tr,td,tr-td,ess,pou,des

def calc_score(m,tr,td,ess,pou,des):
    s=tr-td; score=0; det=[]
    if s>=0: score+=20; det.append({"item":"Saldo positivo","pts":20,"ok":True})
    else: det.append({"item":"Saldo negativo","pts":0,"ok":False})
    if tr>0:
        pp=pou/tr*100
        if pp>=20: score+=20; det.append({"item":"Poupança ≥ 20%","pts":20,"ok":True})
        elif pp>=10: score+=10; det.append({"item":f"Poupança {pp:.0f}%","pts":10,"ok":True})
        else: det.append({"item":f"Poupança {pp:.0f}%","pts":0,"ok":False})
        ep=ess/tr*100
        if ep<=50: score+=20; det.append({"item":"Essenciais ≤ 50%","pts":20,"ok":True})
        elif ep<=60: score+=10; det.append({"item":f"Essenciais {ep:.0f}%","pts":10,"ok":True})
        else: det.append({"item":f"Essenciais {ep:.0f}%","pts":0,"ok":False})
        dp=des/tr*100
        if dp<=30: score+=15; det.append({"item":"Desejos ≤ 30%","pts":15,"ok":True})
        else: det.append({"item":f"Desejos {dp:.0f}%","pts":0,"ok":False})
    mts=m.get("metas",[]); mp=min(len([x for x in mts if x.get("atual",0)>0])*5,15)
    score+=mp
    if mts: det.append({"item":f"Metas ({len(mts)})","pts":mp,"ok":mp>0})
    pnd=[d for d in m.get("despesas",[]) if d.get("situacao")=="Pendente"]
    if not pnd: score+=10; det.append({"item":"Sem pendentes","pts":10,"ok":True})
    else: det.append({"item":f"{len(pnd)} pendentes","pts":0,"ok":False})
    score=min(max(score,0),100)
    lbl="Excelente 🏆" if score>=80 else "Bom 👍" if score>=60 else "Regular ⚠️" if score>=40 else "Crítico 🚨"
    return {"score":score,"label":lbl,"details":det}

# ══════════════════════ MODELS ════════════════════════════════
class LoginReq(BaseModel):
    user_id: str; password: str
class SetupReq(BaseModel):
    user_id: str; name: str; email: str; password: str; color: str = "#2ecc85"
class InviteReq(BaseModel):
    user_id: str; name: str; email: str; color: str = "#2ecc85"
class ChangePasswordReq(BaseModel):
    old_password: str; new_password: str
class FirstPasswordReq(BaseModel):
    new_password: str
class ForgotReq(BaseModel):
    email: str
class ResetReq(BaseModel):
    token: str; new_password: str
class Receita(BaseModel):
    nome: str; valor: float; data_credito: str = ""
class Despesa(BaseModel):
    nome: str; categoria: str = "Essencial"; prestacao: str = ""; valor: float
    vencimento: str = ""; situacao: str = ""; data_pgto: str = ""
    obs: str = ""; ordem: int = 0; recorrente: bool = False
class Poupanca(BaseModel):
    saldo_anterior: float; aporte: float; juros: float
class Compra(BaseModel):
    item: str; valor: float; data: str = ""
class Meta(BaseModel):
    nome: str; alvo: float; atual: float = 0.0; prazo: str = ""
class GlobalMeta(BaseModel):
    nome: str; alvo: float; atual: float = 0.0
    prazo: str = ""; descricao: str = ""; auto_track: bool = False
class PluggyConfig(BaseModel):
    client_id: str; client_secret: str

# ══════════════════════ HTML ══════════════════════════════════
@app.get("/", response_class=HTMLResponse)
async def frontend():
    if STATIC_FILE.exists():
        return HTMLResponse(STATIC_FILE.read_text("utf-8"))
    return HTMLResponse("<h1>index.html não encontrado</h1>", 404)

# ══════════════════════ AUTH ROUTES ══════════════════════════
@app.get("/auth/status")
async def auth_status(session_token: Optional[str] = Cookie(default=None)):
    try: users = db_list_users()
    except: users = []
    if not users:
        return {"setup": True, "authenticated": False, "user": None}
    uid = get_session_uid(session_token)
    if uid:
        u = db_get_user(uid)
        if u:
            return {"setup": False, "authenticated": True,
                    "must_change_password": u.get("must_change_password", False),
                    "user": {"id": uid, "name": u["name"], "email": u.get("email",""),
                             "color": u.get("color","#2ecc85"), "role": u.get("role","user")}}
    return {"setup": False, "authenticated": False, "user": None}

@app.post("/auth/setup")
async def setup(req: SetupReq, response: Response):
    try:
        if db_list_users(): raise HTTPException(400, "Já configurado")
    except HTTPException: raise
    except Exception as e:
        print(f"⚠ setup db_list_users error: {e}")
    salt = secrets.token_hex(16)
    db_save_user({"id":req.user_id,"name":req.name,"email":req.email,"salt":salt,
                  "password_hash":hash_pw(req.password,salt),"color":req.color,
                  "role":"admin","must_change_password":False})
    token = create_session(req.user_id)
    response.set_cookie("session_token", token, httponly=True, samesite="lax", max_age=86400*30)
    return {"ok":True,"user":{"id":req.user_id,"name":req.name,"color":req.color}}

@app.post("/auth/login")
async def login(req: LoginReq, response: Response):
    u = db_get_user(req.user_id)
    if not u or hash_pw(req.password, u["salt"]) != u["password_hash"]:
        raise HTTPException(401, "Usuário ou senha incorretos")
    token = create_session(req.user_id)
    response.set_cookie("session_token", token, httponly=True, samesite="lax", max_age=86400*30)
    return {"ok":True,"must_change_password":u.get("must_change_password",False),
            "user":{"id":req.user_id,"name":u["name"],"email":u.get("email",""),
                    "color":u.get("color","#2ecc85"),"role":u.get("role","user")}}

@app.post("/auth/logout")
async def logout(response: Response, session_token: Optional[str] = Cookie(default=None)):
    if session_token in _sessions: del _sessions[session_token]
    response.delete_cookie("session_token"); return {"ok": True}

@app.post("/auth/change-password")
async def change_pw(req: ChangePasswordReq, uid: str = Depends(require_auth)):
    u = db_get_user(uid)
    if not u or hash_pw(req.old_password, u["salt"]) != u["password_hash"]:
        raise HTTPException(400, "Senha atual incorreta")
    salt = secrets.token_hex(16)
    u["salt"] = salt; u["password_hash"] = hash_pw(req.new_password, salt)
    u["must_change_password"] = False; db_save_user(u); return {"ok": True}

@app.post("/auth/set-first-password")
async def set_first_pw(req: FirstPasswordReq, uid: str = Depends(require_auth)):
    u = db_get_user(uid)
    if not u: raise HTTPException(404)
    if not u.get("must_change_password"): raise HTTPException(400, "Não necessário")
    if len(req.new_password) < 6: raise HTTPException(400, "Mínimo 6 caracteres")
    salt = secrets.token_hex(16)
    u["salt"] = salt; u["password_hash"] = hash_pw(req.new_password, salt)
    u["must_change_password"] = False; db_save_user(u); return {"ok": True}

@app.post("/auth/forgot-password")
async def forgot_pw(req: ForgotReq):
    u = db_get_user_by_email(req.email)
    if u:
        token = secrets.token_urlsafe(32)
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM reset_tokens WHERE user_id=%s", (u["id"],))
                cur.execute("INSERT INTO reset_tokens (token,user_id,expires_at) VALUES (%s,%s,%s)",
                           (token, u["id"], datetime.now()+timedelta(hours=1)))
                conn.commit()
        link = f"{APP_URL}/reset-password?token={token}"
        send_email(u["email"], "Redefinir senha — Orçamento Mensal", _reset_html(u["name"], link))
    return {"ok": True, "message": "Se o e-mail estiver cadastrado, você receberá as instruções."}

@app.post("/auth/reset-password")
async def reset_pw(req: ResetReq):
    if len(req.new_password) < 6: raise HTTPException(400, "Mínimo 6 caracteres")
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM reset_tokens WHERE token=%s AND used=FALSE AND expires_at>NOW()", (req.token,))
            row = cur.fetchone()
            if not row: raise HTTPException(400, "Token inválido ou expirado")
            u = db_get_user(row["user_id"])
            if not u: raise HTTPException(404)
            salt = secrets.token_hex(16)
            u["salt"] = salt; u["password_hash"] = hash_pw(req.new_password, salt)
            u["must_change_password"] = False; db_save_user(u)
            cur.execute("UPDATE reset_tokens SET used=TRUE WHERE token=%s", (req.token,))
            conn.commit()
    return {"ok": True}

@app.get("/reset-password", response_class=HTMLResponse)
async def reset_page(token: str = Query(...)):
    return HTMLResponse(f"""<!DOCTYPE html><html><head><meta charset="UTF-8"/>
    <script>window.location.href='/?reset_token={token}';</script>
    </head><body>Redirecionando...</body></html>""")

# ══════════════════════ PERFIS ════════════════════════════════
@app.get("/api/profiles")
async def list_profiles(uid: str = Depends(require_auth)):
    return [{"id":u["id"],"name":u["name"],"email":u.get("email",""),
             "color":u.get("color","#2ecc85"),"role":u.get("role","user"),
             "must_change_password":u.get("must_change_password",False)}
            for u in db_list_users()]

@app.post("/api/profiles/invite")
async def invite_user(req: InviteReq, uid: str = Depends(require_admin)):
    if db_get_user(req.user_id): raise HTTPException(400, "ID já existe")
    if req.email and db_get_user_by_email(req.email): raise HTTPException(400, "E-mail já cadastrado")
    temp_pass = secrets.token_urlsafe(8)
    salt = secrets.token_hex(16)
    inviter = db_get_user(uid)
    db_save_user({"id":req.user_id,"name":req.name,"email":req.email,"salt":salt,
                  "password_hash":hash_pw(temp_pass,salt),"color":req.color,
                  "role":"user","must_change_password":True})
    sent = False
    if req.email:
        sent = send_email(req.email, "Convite — Orçamento Mensal",
                         _invite_html(req.name, inviter["name"] if inviter else "Admin", APP_URL, temp_pass))
    return {"ok":True,"email_sent":sent,
            "temp_pass": None if sent else temp_pass,
            "message": f"Conta criada. {'E-mail enviado!' if sent else 'E-mail não configurado — senha temporária: '+temp_pass}"}

@app.delete("/api/profiles/{target_id}")
async def delete_profile(target_id: str, uid: str = Depends(require_admin)):
    if target_id == uid: raise HTTPException(400, "Não pode excluir a si mesmo")
    db_delete_user(target_id); return {"ok": True}

@app.put("/api/profiles/{target_id}/reset-password")
async def admin_reset_pw(target_id: str, uid: str = Depends(require_admin)):
    u = db_get_user(target_id)
    if not u: raise HTTPException(404)
    if not u.get("email"): raise HTTPException(400, "Usuário sem e-mail")
    token = secrets.token_urlsafe(32)
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM reset_tokens WHERE user_id=%s", (target_id,))
            cur.execute("INSERT INTO reset_tokens (token,user_id,expires_at) VALUES (%s,%s,%s)",
                       (token, target_id, datetime.now()+timedelta(hours=24)))
            conn.commit()
    u["must_change_password"] = True; db_save_user(u)
    link = f"{APP_URL}/reset-password?token={token}"
    sent = send_email(u["email"], "Redefinição de senha — Orçamento Mensal", _reset_html(u["name"], link))
    return {"ok": True, "email_sent": sent}

# ══════════════════════ MESES ════════════════════════════════
@app.get("/api/months")
async def get_months(uid: str = Depends(require_auth)):
    return db_list_months(uid)

@app.get("/api/month/{key}")
async def get_month(key: str, uid: str = Depends(require_auth)):
    return ensure_month(uid, key)

@app.put("/api/month/{key}")
async def save_month(key: str, payload: dict, uid: str = Depends(require_auth)):
    db_save_month(uid, key, payload); return {"ok": True}

@app.post("/api/month/{key}/receitas")
async def add_receita(key: str, r: Receita, uid: str = Depends(require_auth)):
    m = ensure_month(uid, key); m["receitas"].append(r.model_dump()); db_save_month(uid,key,m); return {"ok":True}

@app.put("/api/month/{key}/receitas/{idx}")
async def upd_receita(key: str, idx: int, r: Receita, uid: str = Depends(require_auth)):
    m = ensure_month(uid, key); m["receitas"][idx] = r.model_dump(); db_save_month(uid,key,m); return {"ok":True}

@app.delete("/api/month/{key}/receitas/{idx}")
async def del_receita(key: str, idx: int, uid: str = Depends(require_auth)):
    m = ensure_month(uid, key); m["receitas"].pop(idx); db_save_month(uid,key,m); return {"ok":True}

@app.post("/api/month/{key}/despesas")
async def add_despesa(key: str, d: Despesa, uid: str = Depends(require_auth)):
    m = ensure_month(uid, key); m["despesas"].append(d.model_dump()); db_save_month(uid,key,m); return {"ok":True}

@app.put("/api/month/{key}/despesas")
async def replace_despesas(key: str, despesas: list[Despesa], uid: str = Depends(require_auth)):
    m = ensure_month(uid, key); m["despesas"] = [d.model_dump() for d in despesas]; db_save_month(uid,key,m); return {"ok":True}

@app.put("/api/month/{key}/despesas/{idx}")
async def upd_despesa(key: str, idx: int, d: Despesa, uid: str = Depends(require_auth)):
    m = ensure_month(uid, key); m["despesas"][idx] = d.model_dump(); db_save_month(uid,key,m); return {"ok":True}

@app.delete("/api/month/{key}/despesas/{idx}")
async def del_despesa(key: str, idx: int, uid: str = Depends(require_auth)):
    m = ensure_month(uid, key); m["despesas"].pop(idx); db_save_month(uid,key,m); return {"ok":True}

@app.put("/api/month/{key}/poupanca")
async def upd_poupanca(key: str, p: Poupanca, uid: str = Depends(require_auth)):
    m = ensure_month(uid, key); m["poupanca"] = p.model_dump(); db_save_month(uid,key,m); return {"ok":True}

@app.post("/api/month/{key}/compras")
async def add_compra(key: str, c: Compra, uid: str = Depends(require_auth)):
    m = ensure_month(uid, key); m["compras"].append(c.model_dump()); db_save_month(uid,key,m); return {"ok":True}

@app.delete("/api/month/{key}/compras/{idx}")
async def del_compra(key: str, idx: int, uid: str = Depends(require_auth)):
    m = ensure_month(uid, key); m["compras"].pop(idx); db_save_month(uid,key,m); return {"ok":True}

@app.post("/api/month/{key}/metas")
async def add_meta(key: str, mt: Meta, uid: str = Depends(require_auth)):
    m = ensure_month(uid, key); m["metas"].append(mt.model_dump()); db_save_month(uid,key,m); return {"ok":True}

@app.put("/api/month/{key}/metas/{idx}")
async def upd_meta(key: str, idx: int, mt: Meta, uid: str = Depends(require_auth)):
    m = ensure_month(uid, key); m["metas"][idx] = mt.model_dump(); db_save_month(uid,key,m); return {"ok":True}

@app.delete("/api/month/{key}/metas/{idx}")
async def del_meta(key: str, idx: int, uid: str = Depends(require_auth)):
    m = ensure_month(uid, key); m["metas"].pop(idx); db_save_month(uid,key,m); return {"ok":True}

@app.post("/api/month/{key}/copy-from/{src}")
async def copy_from(key: str, src: str, uid: str = Depends(require_auth)):
    sm = db_load_month(uid, src)
    if not sm: raise HTTPException(404)
    m = ensure_month(uid, key)
    m["despesas"] = [{**d,"valor":0.0,"situacao":"","data_pgto":"","obs":""} for d in sm.get("despesas",[])]
    db_save_month(uid, key, m); return {"ok": True}

# ══════════════════════ INTELIGÊNCIA ══════════════════════════
@app.get("/api/month/{key}/score")
async def get_score(key: str, uid: str = Depends(require_auth)):
    m=ensure_month(uid,key); tr,td,s,ess,pou,des=month_totals(m); return calc_score(m,tr,td,ess,pou,des)

@app.get("/api/month/{key}/forecast")
async def get_forecast(key: str, uid: str = Depends(require_auth)):
    months=db_list_months(uid); prev=[r["key"] for r in months if r["key"]<key][-3:]
    if not prev: return {"available":False}
    avg=sum(sum(d["valor"] for d in migrate(db_load_month(uid,k)).get("despesas",[])) for k in prev)/len(prev)
    cur=ensure_month(uid,key); cs=sum(d["valor"] for d in cur.get("despesas",[])); cr=sum(r["valor"] for r in cur.get("receitas",[]))
    today=date.today(); days=cal_mod.monthrange(today.year,today.month)[1]
    proj=(cs/max(today.day,1))*days
    return {"available":True,"current_spend":cs,"projected":proj,"avg_historical":avg,"expected_rec":cr,"projected_saldo":cr-proj,"day":today.day,"days_total":days,"alert":proj>avg*1.15}

@app.get("/api/month/{key}/compare")
async def compare_month(key: str, uid: str = Depends(require_auth)):
    months=db_list_months(uid); prev=[r for r in months if r["key"]<key]
    if not prev: return {"available":False}
    pk=prev[-1]["key"]; cur=ensure_month(uid,key); prv=migrate(db_load_month(uid,pk))
    tr_c,td_c,s_c,*_=month_totals(cur); tr_p,td_p,s_p,*_=month_totals(prv)
    return {"available":True,"prev_key":pk,
            "receitas":{"atual":tr_c,"anterior":tr_p,"diff":tr_c-tr_p},
            "despesas":{"atual":td_c,"anterior":td_p,"diff":td_c-td_p},
            "saldo":{"atual":s_c,"anterior":s_p,"diff":s_c-s_p}}

@app.get("/api/month/{key}/notifications")
async def get_notif(key: str, uid: str = Depends(require_auth)):
    m=ensure_month(uid,key); today=date.today(); alerts=[]
    for d in m.get("despesas",[]):
        if d.get("situacao")=="Pago" or not d.get("vencimento") or d["valor"]<=0: continue
        try:
            pts=d["vencimento"].replace("-","/").split("/")
            if len(pts)>=2:
                venc=date(int(pts[2]) if len(pts)>2 else today.year,int(pts[1]),int(pts[0]))
                diff=(venc-today).days
                if diff<=5: alerts.append({"nome":d["nome"],"valor":d["valor"],"vencimento":d["vencimento"],"dias":diff,"urgente":diff<=2})
        except: pass
    return {"notifications":alerts,"count":len(alerts)}

@app.get("/api/month/{key}/trends")
async def get_trends(key: str, uid: str = Depends(require_auth)):
    months=db_list_months(uid); prev=[r["key"] for r in months if r["key"]<key][-3:]
    if len(prev)<2: return {"available":False,"trends":[]}
    def ct(k):
        mv=migrate(db_load_month(uid,k)); out={}
        for d in mv.get("despesas",[]): out[d.get("categoria","Outros")]=out.get(d.get("categoria","Outros"),0)+d["valor"]
        return out
    last=ct(prev[-1]); before=ct(prev[-2]); trends=[]
    for cat,val in last.items():
        old=before.get(cat,0)
        if old>0 and val>0:
            pct=(val-old)/old*100
            if abs(pct)>=10: trends.append({"categoria":cat,"variacao":round(pct,1),"valor_atual":val,"valor_anterior":old,"crescimento":pct>0})
    trends.sort(key=lambda x:abs(x["variacao"]),reverse=True)
    return {"available":True,"trends":trends[:5],"periodo":f"{prev[-2]} → {prev[-1]}"}

@app.get("/api/calendar/{key}")
async def get_calendar(key: str, uid: str = Depends(require_auth)):
    m=ensure_month(uid,key); mm,yy=key.split(".")
    di=cal_mod.monthrange(int(yy),int(mm))[1]; ws,_=cal_mod.monthrange(int(yy),int(mm))
    by_day={}
    for d in m.get("despesas",[]):
        if not d.get("vencimento") or d["valor"]<=0: continue
        try:
            day=int(d["vencimento"].split("/")[0])
            if 1<=day<=di: by_day.setdefault(day,[]).append({"nome":d["nome"][:25],"valor":d["valor"],"situacao":d.get("situacao",""),"categoria":d.get("categoria","")})
        except: pass
    return {"weekday_start":ws,"days_in_month":di,"by_day":{str(k):v for k,v in by_day.items()},"month":mm,"year":yy}

@app.get("/api/annual/{ano}")
async def get_annual(ano: str, uid: str = Depends(require_auth)):
    rows=[]
    for r in db_list_months(uid):
        if not r["key"].endswith(f".{ano}"): continue
        m=migrate(db_load_month(uid,r["key"])); tr,td,s,ess,pou,des=month_totals(m); p=m["poupanca"]
        rows.append({"mes":r["key"],"receitas":tr,"despesas":td,"saldo":s,"essencial":ess,"poupanca":pou,"desejo":des,"poupanca_acum":p["saldo_anterior"]+p["aporte"]+p["juros"]})
    return rows

@app.get("/api/period-analysis")
async def period_analysis(start: str=Query(...), end: str=Query(...), uid: str=Depends(require_auth)):
    keys=[r["key"] for r in db_list_months(uid) if start<=r["key"]<=end]
    if not keys: raise HTTPException(404)
    bc={}; bn={}; tr_t=0.0; td_t=0.0
    for k in keys:
        m=migrate(db_load_month(uid,k)); tr,td,*_=month_totals(m); tr_t+=tr; td_t+=td
        for d in m.get("despesas",[]):
            if d["valor"]>0:
                cat=d.get("categoria","Outros"); bc[cat]=bc.get(cat,0)+d["valor"]
                bn[d["nome"]]=bn.get(d["nome"],0)+d["valor"]
    return {"meses":keys,"total_receitas":tr_t,"total_despesas":td_t,"saldo":tr_t-td_t,"por_categoria":bc,"top_despesas":[{"nome":n,"valor":v} for n,v in sorted(bn.items(),key=lambda x:x[1],reverse=True)[:10]]}

@app.get("/api/family/{key}")
async def family_view(key: str, uid: str = Depends(require_auth)):
    users=db_list_users(); result=[]
    for u in users:
        m=migrate(db_load_month(u["id"],key) or default_month()); tr,td,s,ess,pou,des=month_totals(m)
        result.append({"user_id":u["id"],"name":u["name"],"color":u.get("color","#2ecc85"),"receitas":tr,"despesas":td,"saldo":s,"essencial":ess,"poupanca":pou,"desejo":des})
    return {"profiles":result,"total_receitas":sum(r["receitas"] for r in result),"total_despesas":sum(r["despesas"] for r in result),"saldo_familiar":sum(r["saldo"] for r in result)}

# ══════════════════════ METAS GLOBAIS ════════════════════════
@app.get("/api/global-metas")
async def get_gm(uid: str = Depends(require_auth)):
    metas=db_get_global_metas(uid); months=db_list_months(uid)
    for mt in metas:
        if mt.get("auto_track"):
            mt["atual_calculado"]=sum(migrate(db_load_month(uid,r["key"]))["poupanca"].get("aporte",0) for r in months)
    return metas

@app.post("/api/global-metas")
async def add_gm(m: GlobalMeta, uid: str = Depends(require_auth)):
    metas=db_get_global_metas(uid); metas.append(m.model_dump()); db_save_global_metas(uid,metas); return {"ok":True}

@app.put("/api/global-metas/{idx}")
async def upd_gm(idx: int, m: GlobalMeta, uid: str = Depends(require_auth)):
    metas=db_get_global_metas(uid); metas[idx]=m.model_dump(); db_save_global_metas(uid,metas); return {"ok":True}

@app.delete("/api/global-metas/{idx}")
async def del_gm(idx: int, uid: str = Depends(require_auth)):
    metas=db_get_global_metas(uid); metas.pop(idx); db_save_global_metas(uid,metas); return {"ok":True}

# ══════════════════════ EXTRATO CSV ══════════════════════════
@app.post("/api/import-extrato")
async def import_extrato(file: UploadFile=File(...), uid: str=Depends(require_auth)):
    import csv as csv_mod
    raw=await file.read(); enc=chardet.detect(raw)["encoding"] or "latin-1"
    try: content=raw.decode(enc)
    except: content=raw.decode("latin-1",errors="replace")
    lines=content.splitlines(); hi=0
    for i,ln in enumerate(lines):
        if "data" in ln.lower() and any(x in ln.lower() for x in ["valor","histórico","historico","descrição","descricao"]):
            hi=i; break
    body="\n".join(lines[hi:]); rows=[]
    for sep in [";",","]:
        try:
            r=list(csv_mod.DictReader(io.StringIO(body),delimiter=sep))
            if r and len(r[0])>=3: rows=r; break
        except: continue
    if not rows: raise HTTPException(400,"Não foi possível ler o CSV")
    def norm(k): return (k or "").strip().lower().replace("ç","c").replace("ã","a").replace("é","e").replace(" ","_").replace(".","")
    rows=[{norm(k):(v or "").strip() for k,v in r.items()} for r in rows]
    cd=next((k for k in rows[0] if "data" in k),None); cdesc=next((k for k in rows[0] if any(x in k for x in ["historico","descri","memo"])),None)
    cv=next((k for k in rows[0] if "valor" in k),None); ct=next((k for k in rows[0] if "tipo" in k or "debito" in k),None)
    if not cv: raise HTTPException(400,"Coluna 'Valor' não encontrada")
    deb,cred=[],[]
    for row in rows:
        val=clean_val(row.get(cv,"0")); desc=str(row.get(cdesc,"")).strip() if cdesc else ""; data=str(row.get(cd,"")).strip() if cd else ""; tipo=str(row.get(ct,"")).lower() if ct else ""
        is_d=("deb" in tipo or "saida" in tipo) if ct else val<0
        item={"descricao":desc,"valor":abs(val),"data":data,"categoria":guess_cat(desc)}
        if abs(val)>0: (deb if is_d else cred).append(item)
    return {"debitos":deb,"creditos":cred,"banco_detectado":"Auto"}

@app.post("/api/month/{key}/check-duplicates")
async def check_dups(key: str, items: list[dict], uid: str=Depends(require_auth)):
    m=ensure_month(uid,key)
    existing=[(d["nome"].lower()[:20],d["valor"]) for d in m.get("despesas",[]) if d["valor"]>0]
    return [{**item,"possivel_duplicata":any(abs(v-float(item.get("valor",0)))<0.01 and (item.get("descricao","").lower()[:10] in n or n[:10] in item.get("descricao","").lower()[:10]) for n,v in existing)} for item in items]

# ══════════════════════ PDF FATURA ════════════════════════════
SKIP_WORDS=["total cart","pagamento minimo","pagamento mínimo","saldo total","valor total","fatura atual","despesas do mes","despesas do mês","limite de","proxima fatura","encargos financeiros","data de corte","vencimento","subtotal"]

def _skip(desc): return any(s in desc.lower() for s in SKIP_WORDS) or len(desc)<3

def _detect_bank(text):
    low=text.lower()
    if any(x in low for x in ["banco inter","bancointer","bco inter"]): return "Inter"
    if "nubank" in low or "nu pagamentos" in low: return "Nubank"
    if "bradesco" in low: return "Bradesco"
    if "itau" in low or "itaú" in low: return "Itaú"
    if "santander" in low: return "Santander"
    if "caixa" in low: return "Caixa"
    return "Banco"

def _parse_inter(text):
    PAT=re.compile(r"(\d{2}\s+de\s+\w+\.?\s+\d{4})\s+(.+?)\s+-\s+([+]?\s*R?\$?\s*[\d\.]+,\d{2})",re.IGNORECASE)
    results=[]; seen=set()
    for m in PAT.finditer(text):
        data,desc,vs=m.group(1).strip(),m.group(2).strip(),m.group(3)
        if _skip(desc): continue
        val=abs(clean_val(vs))
        if val<=0 or val>200000: continue
        is_c="+" in vs; k=f"{data[:8]}|{desc[:15]}|{round(val,2)}"
        if k in seen: continue
        seen.add(k); results.append({"data":data,"descricao":desc[:60],"valor":val,"is_credito":is_c,"categoria":guess_cat(desc)})
    return results

def _extract_text(pdf_bytes, pw):
    text=""
    try:
        import pdfplumber
        with pdfplumber.open(io.BytesIO(pdf_bytes), password=pw or None) as pdf:
            for page in pdf.pages:
                try:
                    for tbl in (page.extract_tables() or []):
                        for row in tbl: text+=" ".join(str(c or "").strip() for c in row if c)+"\n"
                    t=page.extract_text()
                    if t: text+=t+"\n"
                except: pass
    except: pass
    if not text.strip():
        try:
            import pypdf; reader=pypdf.PdfReader(io.BytesIO(pdf_bytes))
            if reader.is_encrypted:
                for p in [pw, (pw or "").replace("-","").replace(".",""), ""]:
                    try:
                        if reader.decrypt(p or ""): break
                    except: pass
            for page in reader.pages:
                try: text+=page.extract_text()+"\n"
                except: pass
        except Exception as e: raise ValueError(f"Não foi possível abrir o PDF: {e}")
    return text

@app.post("/api/import-pdf")
async def import_pdf(file: UploadFile=File(...), password: str=Query(default=""), uid: str=Depends(require_auth)):
    raw=await file.read()
    if not raw: raise HTTPException(400,"Arquivo vazio")
    try:
        text=_extract_text(raw, password)
        if not text.strip(): raise ValueError("PDF sem texto extraível. Tente o CSV.")
        banco=_detect_bank(text); raw_items=_parse_inter(text)
        if not raw_items:
            PATS=[r"^(\d{2}/\d{2}/\d{4})\s+(.{3,60}?)\s+R?\$?\s*([\d\.]+,\d{2})\s*$",r"^(\d{2}/\d{2})\s+(.{3,60}?)\s+R?\$?\s*([\d\.]+,\d{2})\s*$"]
            seen=set()
            for line in text.splitlines():
                line=line.strip()
                if len(line)<8: continue
                for pat in PATS:
                    m=re.match(pat,line,re.IGNORECASE)
                    if not m: continue
                    data,desc,vs=m.group(1),m.group(2).strip(),m.group(3)
                    if _skip(desc): continue
                    val=clean_val(vs)
                    if val<=0 or val>50000: continue
                    k=f"{data[:8]}|{desc[:15]}|{round(val,2)}"
                    if k in seen: continue
                    seen.add(k); raw_items.append({"data":data,"descricao":desc[:60],"valor":val,"is_credito":False,"categoria":guess_cat(desc)}); break
        seen2=set(); final=[]
        for r in raw_items:
            k=f"{r['data'][:8]}|{r['descricao'][:15]}|{round(r['valor'],2)}"
            if k not in seen2: seen2.add(k); final.append(r)
        deb=[{k:v for k,v in r.items() if k!="is_credito"} for r in final if not r["is_credito"]]
        cre=[{k:v for k,v in r.items() if k!="is_credito"} for r in final if r["is_credito"]]
        if not deb and not cre: raise ValueError(f"Nenhuma transação encontrada no PDF do {banco}.")
        return {"debitos":deb,"creditos":cre,"banco":banco}
    except ValueError as e: raise HTTPException(400,str(e))
    except Exception as e: raise HTTPException(500,f"Erro: {e}")

# ══════════════════════ BACKUPS ══════════════════════════════
@app.get("/api/backups")
async def list_backups(uid: str=Depends(require_auth)):
    baks=sorted(BACKUP_DIR.glob(f"backup_{uid}_*.json"),reverse=True)[:20]
    return [{"filename":b.name,"size_kb":round(b.stat().st_size/1024,1),"date":b.stat().st_mtime} for b in baks]

@app.post("/api/backups/restore/{filename}")
async def restore_backup(filename: str, uid: str=Depends(require_auth)):
    f=BACKUP_DIR/filename
    if not f.exists() or uid not in filename: raise HTTPException(404)
    data=json.loads(f.read_text("utf-8"))
    if "receitas" in data:
        parts=filename.replace(f"backup_{uid}_","").split("_")
        if len(parts)>=2: db_save_month(uid,parts[0],data)
    return {"ok":True}

# ══════════════════════ EXPORT ════════════════════════════════
@app.get("/api/month/{key}/excel")
async def month_excel(key: str, uid: str=Depends(require_auth)):
    try: from openpyxl import Workbook; from openpyxl.styles import Font
    except: raise HTTPException(500,"openpyxl não instalado")
    m=ensure_month(uid,key); tr,td,s,ess,pou,des=month_totals(m)
    wb=Workbook(); ws=wb.active; ws.title="Resumo"
    ws['A1']=f"Orçamento — {key}"; ws['A1'].font=Font(bold=True,size=14)
    for i,(l,v) in enumerate([("Receitas",tr),("Despesas",td),("Saldo",s),("Essenciais",ess),("Poupança",pou),("Desejos",des)],3):
        ws.cell(i,1,l).font=Font(bold=True); ws.cell(i,2,v).number_format='R$ #,##0.00'
    ws2=wb.create_sheet("Receitas"); ws2.append(["Nome","Valor","Data"])
    for r in m.get("receitas",[]): ws2.append([r["nome"],r["valor"],r.get("data_credito","")]); ws2.cell(ws2.max_row,2).number_format='R$ #,##0.00'
    ws3=wb.create_sheet("Despesas"); ws3.append(["Nome","Categoria","Valor","Vencimento","Situação","Recorrente"])
    for d in m.get("despesas",[]): ws3.append([d["nome"],d.get("categoria",""),d["valor"],d.get("vencimento",""),d.get("situacao",""),"Sim" if d.get("recorrente") else "Não"]); ws3.cell(ws3.max_row,3).number_format='R$ #,##0.00'
    ws4=wb.create_sheet("Compras"); ws4.append(["Item","Valor","Data"])
    for c in m.get("compras",[]): ws4.append([c["item"],c["valor"],c.get("data","")]); ws4.cell(ws4.max_row,2).number_format='R$ #,##0.00'
    buf=io.BytesIO(); wb.save(buf); buf.seek(0)
    return StreamingResponse(buf, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                             headers={"Content-Disposition":f"attachment; filename=orcamento_{key}.xlsx"})

@app.get("/api/month/{key}/pdf")
async def month_pdf(key: str, uid: str=Depends(require_auth)):
    try: from fpdf import FPDF
    except: raise HTTPException(500,"fpdf2 não instalado")
    m=ensure_month(uid,key); tr,td,s,ess,pou,des=month_totals(m)
    pdf=FPDF(); pdf.add_page(); pdf.set_auto_page_break(True,15)
    pdf.set_fill_color(26,71,42); pdf.rect(0,0,210,28,"F")
    pdf.set_font("Helvetica","B",16); pdf.set_text_color(255,255,255)
    pdf.cell(0,10,"",ln=True); pdf.cell(0,10,f"Orcamento Mensal  {key}",align="C",ln=True)
    pdf.set_text_color(0,0,0); pdf.ln(4)
    sc=calc_score(m,tr,td,ess,pou,des); pdf.set_font("Helvetica","B",11)
    pdf.cell(0,7,f"Score: {sc['score']}/100 — {sc['label']}",ln=True); pdf.ln(3)
    for lbl,val,r,g,b in [("Receitas",tr,39,174,96),("Despesas",td,231,76,60),("Saldo",s,39,174,96 if s>=0 else 231,76,60)]:
        pdf.set_fill_color(r,g,b); pdf.set_text_color(255,255,255); pdf.set_font("Helvetica","B",9)
        pdf.cell(60,8,f"  {lbl}",fill=True); pdf.set_text_color(50,50,50); pdf.set_fill_color(245,245,245)
        pdf.cell(55,8,f"  {brl(val)}",fill=True,ln=True)
    pdf.ln(3); pdf.set_font("Helvetica","B",11); pdf.set_text_color(26,71,42)
    pdf.cell(0,7,"Despesas",ln=True); pdf.set_text_color(0,0,0); pdf.set_font("Helvetica","",8)
    CAT_C={"Essencial":(41,128,185),"Poupança":(39,174,96),"Desejo":(230,126,34)}
    for d in m.get("despesas",[]):
        if d["valor"]>0:
            r2,g2,b2=CAT_C.get(d.get("categoria","Essencial"),(120,120,120))
            pdf.set_text_color(r2,g2,b2); pdf.cell(8,5,d.get("categoria","?")[0])
            pdf.set_text_color(0,0,0); pdf.cell(90,5,d["nome"]); pdf.cell(28,5,d.get("situacao","")); pdf.cell(30,5,brl(d["valor"]),ln=True)
    pdf.ln(3); pdf.set_font("Helvetica","I",7); pdf.set_text_color(150,150,150)
    pdf.cell(0,5,f"Gerado em {datetime.now().strftime('%d/%m/%Y %H:%M')}",align="C")
    buf=io.BytesIO(pdf.output())
    return StreamingResponse(buf, media_type="application/pdf",
                             headers={"Content-Disposition":f"attachment; filename=orcamento_{key}.pdf"})

@app.get("/api/annual/{ano}/excel")
async def annual_excel(ano: str, uid: str=Depends(require_auth)):
    try: from openpyxl import Workbook
    except: raise HTTPException(500)
    rows=await get_annual(ano,uid)
    if not rows: raise HTTPException(404)
    wb=Workbook(); ws=wb.active; ws.title=f"Anual {ano}"
    ws.append(["Mês","Receitas","Despesas","Saldo","Essencial","Poupança","Desejo","Poupança Acum."])
    for r in rows:
        ws.append([r["mes"],r["receitas"],r["despesas"],r["saldo"],r["essencial"],r["poupanca"],r["desejo"],r["poupanca_acum"]])
        for ci in range(2,9): ws.cell(ws.max_row,ci).number_format='R$ #,##0.00'
    buf=io.BytesIO(); wb.save(buf); buf.seek(0)
    return StreamingResponse(buf, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                             headers={"Content-Disposition":f"attachment; filename=anual_{ano}.xlsx"})

@app.get("/api/annual/{ano}/pdf")
async def annual_pdf(ano: str, uid: str=Depends(require_auth)):
    try: from fpdf import FPDF
    except: raise HTTPException(500)
    rows=await get_annual(ano,uid)
    if not rows: raise HTTPException(404)
    pdf=FPDF(); pdf.add_page(); pdf.set_auto_page_break(True,15)
    pdf.set_fill_color(26,71,42); pdf.rect(0,0,210,35,"F")
    pdf.set_font("Helvetica","B",18); pdf.set_text_color(255,255,255)
    pdf.cell(0,15,"",ln=True); pdf.cell(0,12,f"Relatorio Anual  {ano}",align="C",ln=True)
    pdf.set_text_color(0,0,0); pdf.ln(5)
    tot_r=sum(r["receitas"] for r in rows); tot_d=sum(r["despesas"] for r in rows)
    pdf.set_font("Helvetica","B",10); pdf.cell(0,7,f"Receita: {brl(tot_r)}  Despesa: {brl(tot_d)}  Saldo: {brl(tot_r-tot_d)}",ln=True); pdf.ln(3)
    pdf.set_font("Helvetica","B",8); pdf.set_fill_color(26,71,42); pdf.set_text_color(255,255,255)
    for h,w in [("Mes",20),("Receitas",38),("Despesas",38),("Saldo",38),("Poupanca",38)]: pdf.cell(w,6,h,fill=True)
    pdf.ln(); pdf.set_text_color(0,0,0)
    for r in rows:
        pdf.set_font("Helvetica","",8); pdf.cell(20,5,r["mes"])
        for v,c in [(r["receitas"],(39,174,96)),(r["despesas"],(231,76,60)),(r["saldo"],(39,174,96) if r["saldo"]>=0 else (231,76,60)),(r["poupanca_acum"],(41,128,185))]:
            pdf.set_text_color(*c); pdf.cell(38,5,brl(v))
        pdf.set_text_color(0,0,0); pdf.ln()
    pdf.ln(3); pdf.set_font("Helvetica","I",7); pdf.set_text_color(150,150,150)
    pdf.cell(0,5,f"Gerado em {datetime.now().strftime('%d/%m/%Y %H:%M')}",align="C")
    buf=io.BytesIO(pdf.output())
    return StreamingResponse(buf, media_type="application/pdf",
                             headers={"Content-Disposition":f"attachment; filename=anual_{ano}.pdf"})

# ══════════════════════ PLUGGY ════════════════════════════════
PLUGGY_BASE="https://api.pluggy.ai"

def _get_pluggy(uid):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM pluggy_config WHERE user_id=%s",(uid,))
            r=cur.fetchone(); return dict(r) if r else None

def _save_pluggy(uid, cfg):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO pluggy_config (user_id,client_id,client_secret,items) VALUES (%s,%s,%s,%s) ON CONFLICT (user_id) DO UPDATE SET client_id=EXCLUDED.client_id,client_secret=EXCLUDED.client_secret,items=EXCLUDED.items",
                       (uid,cfg.get("client_id"),cfg.get("client_secret"),json.dumps(cfg.get("items",[]))))
            conn.commit()

def _pluggy_key(cid,csec):
    if not REQUESTS_OK: raise HTTPException(500,"requests não instalado")
    r=req_lib.post(f"{PLUGGY_BASE}/auth",json={"clientId":cid,"clientSecret":csec},timeout=12)
    r.raise_for_status(); return r.json()["apiKey"]

def _pluggy_req(method,path,key,**kw):
    r=getattr(req_lib,method)(f"{PLUGGY_BASE}{path}",headers={"X-API-KEY":key},timeout=15,**kw)
    if not r.ok: raise HTTPException(r.status_code,r.text[:300])
    return r.json()

@app.get("/api/openfinance/config-status")
async def of_status(uid: str=Depends(require_auth)):
    cfg=_get_pluggy(uid)
    return {"configured":bool(cfg),"client_id":cfg.get("client_id","") if cfg else "","items":list(cfg.get("items",[])) if cfg else []}

@app.post("/api/openfinance/config")
async def of_config(config: PluggyConfig, uid: str=Depends(require_auth)):
    _pluggy_key(config.client_id,config.client_secret)
    cfg=_get_pluggy(uid) or {}; cfg.update({"client_id":config.client_id,"client_secret":config.client_secret})
    _save_pluggy(uid,cfg); return {"ok":True}

@app.delete("/api/openfinance/config")
async def of_del_config(uid: str=Depends(require_auth)):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM pluggy_config WHERE user_id=%s",(uid,)); conn.commit()
    return {"ok":True}

@app.post("/api/openfinance/connect-token")
async def of_connect_token(uid: str=Depends(require_auth)):
    cfg=_get_pluggy(uid)
    if not cfg: raise HTTPException(400,"Configure Pluggy primeiro")
    key=_pluggy_key(cfg["client_id"],cfg["client_secret"])
    r=req_lib.post(f"{PLUGGY_BASE}/connect_token",headers={"X-API-KEY":key},json={},timeout=15); r.raise_for_status()
    data=r.json(); token=data.get("accessToken") or data.get("connectToken") or ""
    if not token: raise HTTPException(500,f"Token não encontrado: {list(data.keys())}")
    return {"connect_token":token}

@app.post("/api/openfinance/save-item")
async def of_save_item(body: dict, uid: str=Depends(require_auth)):
    cfg=_get_pluggy(uid) or {}; key=_pluggy_key(cfg["client_id"],cfg["client_secret"]); item_id=body["item_id"]
    try: r=_pluggy_req("get",f"/items/{item_id}",key)
    except: r={"id":item_id,"connector":{},"status":"UPDATED"}
    items=list(cfg.get("items",[]))
    if not any(i["id"]==item_id for i in items):
        items.append({"id":item_id,"connector":r.get("connector",{}),"status":r.get("status","UPDATED"),"added_at":datetime.now().isoformat()})
    cfg["items"]=items; _save_pluggy(uid,cfg); return {"ok":True}

@app.get("/api/openfinance/items/{item_id}/accounts")
async def of_accounts(item_id: str, uid: str=Depends(require_auth)):
    cfg=_get_pluggy(uid)
    if not cfg: raise HTTPException(400)
    return _pluggy_req("get",f"/accounts?itemId={item_id}",_pluggy_key(cfg["client_id"],cfg["client_secret"]))

@app.get("/api/openfinance/accounts/{account_id}/transactions")
async def of_txns(account_id: str, date_from: str=Query(default=""), date_to: str=Query(default=""), uid: str=Depends(require_auth)):
    cfg=_get_pluggy(uid)
    if not cfg: raise HTTPException(400)
    key=_pluggy_key(cfg["client_id"],cfg["client_secret"]); params={"accountId":account_id,"pageSize":200}
    if date_from: params["from"]=date_from
    if date_to: params["to"]=date_to
    return _pluggy_req("get","/transactions",key,params=params)

@app.post("/api/openfinance/accounts/{account_id}/import/{month_key}")
async def of_import(account_id: str, month_key: str, body: dict, uid: str=Depends(require_auth)):
    cfg=_get_pluggy(uid)
    if not cfg: raise HTTPException(400)
    key=_pluggy_key(cfg["client_id"],cfg["client_secret"]); mm,yy=month_key.split(".")
    days=cal_mod.monthrange(int(yy),int(mm))[1]
    txns=_pluggy_req("get","/transactions",key,params={"accountId":account_id,"pageSize":200,"from":f"{yy}-{mm}-01","to":f"{yy}-{mm}-{days:02d}"}).get("results",[])
    sel=set(body.get("ids",[]));
    if sel: txns=[t for t in txns if t["id"] in sel]
    m=ensure_month(uid,month_key); imp_d=imp_c=0
    for t in txns:
        val=abs(float(t.get("amount",0)))
        if val<=0: continue
        desc=(t.get("description") or t.get("merchant",{}).get("name","") or "Transação").strip()
        ds=(t.get("date","")[:10]).replace("-","/")
        is_d=float(t.get("amount",0))<0 or t.get("type","").upper() in ("DEBIT","PAYMENT","PIX_DEBIT","TED","DOC","FEE")
        if is_d: m["despesas"].append({"nome":desc[:60],"categoria":guess_cat(desc),"prestacao":"","valor":val,"vencimento":ds,"situacao":"Pago","data_pgto":ds,"obs":"Pluggy","ordem":999,"recorrente":False}); imp_d+=1
        else: m["receitas"].append({"nome":desc[:50],"valor":val,"data_credito":ds}); imp_c+=1
    db_save_month(uid,month_key,m); return {"ok":True,"despesas":imp_d,"receitas":imp_c,"total":imp_d+imp_c}

# ══════════════════════ MAIN ══════════════════════════════════
if __name__ == "__main__":
    port=int(os.environ.get("PORT",8000))
    print("\n"+"═"*52+"\n  💰 ORÇAMENTO MENSAL v5.0\n  🗄  PostgreSQL + E-mail\n"+"═"*52+"\n")
    uvicorn.run("app:app", host="0.0.0.0", port=port, reload=False)
