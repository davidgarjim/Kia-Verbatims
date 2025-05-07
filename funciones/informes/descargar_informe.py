import base64
import tempfile
import markdown2
import pdfkit
import streamlit as st
import os

def descargar_informe(informe):

    def file_to_base64(path):
        with open(path, "rb") as f:
            return base64.b64encode(f.read()).decode()

    logo_kia_b64 = file_to_base64("media/logokia.png")
    logo_tuinkel_b64 = file_to_base64("media/logotuinkel.png")

    font_light = file_to_base64("media/fonts/KiaSignatureOTFLight.otf")
    font_regular = file_to_base64("media/fonts/KiaSignatureOTFRegular.otf")
    font_bold = file_to_base64("media/fonts/KiaSignatureOTFBold.otf")

    html_body = markdown2.markdown(informe)

    html_main = f"""
    <!DOCTYPE html>
    <html>
    <head>
    <meta charset='utf-8'>
    <style>

    @font-face {{
        font-family: 'KiaSignature';
        src: url(data:font/opentype;charset=utf-8;base64,{font_light}) format('opentype');
        font-weight: 300;
        font-style: normal;
    }}

    @font-face {{
        font-family: 'KiaSignature';
        src: url(data:font/opentype;charset=utf-8;base64,{font_regular}) format('opentype');
        font-weight: normal;
        font-style: normal;
    }}

    @font-face {{
        font-family: 'KiaSignature';
        src: url(data:font/opentype;charset=utf-8;base64,{font_bold}) format('opentype');
        font-weight: bold;
        font-style: normal;
    }}

    body {{
        font-family: 'KiaSignature', "Segoe UI", sans-serif;
        font-size: 14px;
        line-height: 1.8;
        color: #333;
        text-align: justify;
        margin: 100px 50px 100px 50px;
    }}

    strong, b {{
        font-weight: bold;
    }}

    em, i {{
        font-style: italic;
    }}

    </style>
    </head>
    <body>
    {html_body}
    </body>
    </html>
    """

    html_footer = f"""
    <!DOCTYPE html>
    <html>
    <head>
    <style>
    body {{
        margin: 0;
        font-size: 10px;
    }}
    .footer-container {{
        width: 100%;
        height: 50px;
        position: relative;
    }}
    .footer-left {{
        position: absolute;
        left: 40px;
        bottom: 5px;
    }}
    .footer-right {{
        position: absolute;
        right: 30px;
        bottom: 0;
    }}
    </style>
    </head>
    <body>
        <div class="footer-container">
            <img class="footer-right" src="data:image/png;base64,{logo_tuinkel_b64}" height="25">
            <img class="footer-left" src="data:image/png;base64,{logo_kia_b64}" height="30">
        </div>
    </body>
    </html>
    """

    with tempfile.NamedTemporaryFile(delete=False, suffix=".html") as f_main:
        f_main.write(html_main.encode("utf-8"))
        html_main_path = f_main.name

    with tempfile.NamedTemporaryFile(delete=False, suffix=".html") as f_footer:
        f_footer.write(html_footer.encode("utf-8"))
        html_footer_path = f_footer.name

    pdf_path = html_main_path.replace(".html", ".pdf")

    config = pdfkit.configuration(wkhtmltopdf=r"C:\Program Files\wkhtmltopdf\bin\wkhtmltopdf.exe")
    options = {
        'footer-html': html_footer_path,
        'margin-bottom': '30mm',
        'margin-top': '40mm'
    }

    pdfkit.from_file(html_main_path, pdf_path, options=options, configuration=config)

    with open(pdf_path, "rb") as f:
        st.download_button("📄 Descargar PDF", f, file_name="informe.pdf", mime="application/pdf")

    os.unlink(html_footer_path)
    os.unlink(html_main_path)
