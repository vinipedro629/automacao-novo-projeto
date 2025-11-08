import os
import pandas as pd
import threading
import time
from flask import Flask, render_template, request, redirect, url_for, flash, session, jsonify, send_file

UPLOAD_FOLDER = "uploads"
ALLOWED_EXTENSIONS = {"xlsx", "csv", "txt"}

app = Flask(__name__)
app.secret_key = "chave_secreta"
app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

status_execucao = {}

def allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS

def ler_arquivo_planilha(path):
    ext = path.lower().split('.')[-1]
    if ext == "xlsx":
        df = pd.read_excel(path)
    elif ext == "csv":
        df = pd.read_csv(path)
    elif ext == "txt":
        try:
            df = pd.read_csv(path, sep="\t")
        except Exception:
            df = pd.read_csv(path, sep=",")
    else:
        raise ValueError("Formato não suportado: {}".format(ext))
    return df

def executar_automacao(session_id, dados_mapeados):
    total = len(dados_mapeados)
    status_execucao[session_id] = {
        'current': 0,
        'total': total,
        'logs': [],
        'done': False,
        'relatorio_path': None
    }
    relatorio = []
    for idx, registro in enumerate(dados_mapeados):
        time.sleep(1)  # Simulação
        resultado = {
            "linha": idx + 1,
            "dados": registro,
            "status": "Sucesso",
            "mensagem": ""
        }
        try:
            status_execucao[session_id]['logs'].append(f'Sucesso: {registro}')
        except Exception as e:
            resultado["status"] = "Erro"
            resultado["mensagem"] = str(e)
            status_execucao[session_id]['logs'].append(f'Erro: {e}')
        relatorio.append(resultado)
        status_execucao[session_id]['current'] = idx + 1

    df_relatorio = pd.DataFrame([{
        **r['dados'],
        "linha": r["linha"],
        "status": r["status"],
        "mensagem": r["mensagem"]
    } for r in relatorio])
    path = f"uploads/relatorio_{session_id}.csv"
    df_relatorio.to_html(classes="table table-striped table-bordered align-middle", index=False)  # importante para o preview!
    df_relatorio.to_csv(path, index=False, encoding='utf-8-sig')
    status_execucao[session_id]['relatorio_path'] = path
    status_execucao[session_id]['done'] = True

@app.route("/", methods=["GET", "POST"])
def index():
    preview = None
    columns = []
    mapping_needed = False
    selecao_parcial = False
    pronto_automacao = False

    if request.method == "POST":
        if "file" in request.files:
            file = request.files["file"]
            if file.filename == "":
                flash("Nenhum arquivo selecionado!")
                return redirect(request.url)
            if file and allowed_file(file.filename):
                filename = file.filename
                filepath = os.path.join(app.config["UPLOAD_FOLDER"], filename)
                file.save(filepath)
                try:
                    df = ler_arquivo_planilha(filepath)
                    session['filepath'] = filepath
                    columns = list(df.columns)
                    preview = df.to_html(classes="table table-striped table-bordered align-middle", index=False)
                    mapping_needed = True
                    flash(f"Arquivo {filename} processado com sucesso! Selecione as colunas para automatização.")
                except Exception as e:
                    flash(f"Erro ao processar arquivo: {e}")
            else:
                flash("Tipo de arquivo não suportado!")
        elif "mapping" in request.form:
            filepath = session.get('filepath')
            if not filepath or not os.path.exists(filepath):
                flash("Arquivo não encontrado para mapeamento.")
                return redirect(url_for("index"))
            df = ler_arquivo_planilha(filepath)
            columns = list(df.columns)
            colunas_selecionadas = request.form.getlist("colunas_selecionadas")
            if not colunas_selecionadas:
                preview = df.to_html(classes="table table-striped table-bordered align-middle", index=False)
                flash("Selecione ao menos uma coluna.")
                mapping_needed = True
                return render_template("index.html", preview=preview, columns=columns, mapping_needed=mapping_needed)
            novos_nomes = []
            for col in colunas_selecionadas:
                novo_nome = request.form.get(f"novo_nome_{col}", col)
                novos_nomes.append(novo_nome if novo_nome else col)
            dados_mapeados = df[colunas_selecionadas].copy()
            dados_mapeados.columns = novos_nomes
            session['dados_mapeados'] = dados_mapeados.to_dict(orient='records')
            session['selected_columns'] = novos_nomes
            selecao_parcial = True
            return render_template("index.html", dados=dados_mapeados.to_dict(orient='records'), columns=novos_nomes, selecao_parcial=selecao_parcial)
        elif "filtrar_e_processar" in request.form:
            all_data = session.get('dados_mapeados', [])
            colunas = session.get('selected_columns', [])
            selecionados = [int(idx) for idx in request.form.getlist('selected')]
            dados_filtrados = [row for idx, row in enumerate(all_data) if idx in selecionados]
            session['dados_mapeados'] = dados_filtrados
            pronto_automacao = True
            dados_preview = pd.DataFrame(dados_filtrados)
            preview = dados_preview.to_html(classes="table table-striped table-bordered align-middle", index=False)
            return render_template("index.html", preview=preview, columns=colunas, pronto_automacao=pronto_automacao)

    return render_template("index.html", preview=preview, columns=columns, mapping_needed=mapping_needed)

@app.route("/start_automacao", methods=["POST"])
def start_automacao():
    dados_mapeados = session.get('dados_mapeados', [])
    session_id = str(time.time())
    threading.Thread(target=executar_automacao, args=(session_id, dados_mapeados), daemon=True).start()
    return jsonify({"session_id": session_id})

@app.route("/status_automacao/<session_id>")
def status_automacao(session_id):
    status = status_execucao.get(session_id, {})
    stats = {}
    if status.get("relatorio_path") and os.path.exists(status["relatorio_path"]):
        df = pd.read_csv(status["relatorio_path"])
        stats["sucesso"] = int((df["status"] == "Sucesso").sum())
        stats["erro"] = int((df["status"] == "Erro").sum())
        for col in df.columns:
            if col not in ["linha", "status", "mensagem"] and col.lower() not in ["nome", "data"]:
                if df[col].nunique() > 1 and df[col].nunique() <= 15:
                    stats[col] = df[col].value_counts().to_dict()
    status["stats"] = stats
    return jsonify(status)

@app.route("/download_relatorio/<session_id>")
def download_relatorio(session_id):
    s = status_execucao.get(session_id)
    if s and s.get("relatorio_path") and os.path.exists(s["relatorio_path"]):
        return send_file(s["relatorio_path"],
                         mimetype="text/csv",
                         as_attachment=True,
                         download_name=f"relatorio_{session_id}.csv")
    return "Relatório não disponível.", 404

if __name__ == "__main__":
    app.run(debug=True)
