import os
import asyncio
from dataclasses import dataclass, field
from typing import List
from string import Template
import html as html_escape

from pyppeteer import launch


@dataclass
class CollageInput:
    crm_id: str
    complex_name: str = ''
    address: str = ''
    area_sqm: str = ''
    floor: str = ''
    housing_class: str = ''
    price: str = ''
    rooms: str = ''
    benefits: List[str] = field(default_factory=list)
    photos: List[str] = field(default_factory=list)
    client_name: str = ''
    rop: str = ''
    mop: str = ''
    agent_phone: str = ''
    action_banner: str = ''
    # Тип объекта для коллажа: "Квартира" (по умолчанию) или "Коммерческое помещение"
    object_type: str = 'Квартира'


COLLAGE_TEMPLATE = """
<!DOCTYPE html>
<html lang="ru">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Коллаж</title>
  <style>
    :root {
      --teal1:#19b7cf;
      --teal2:#0a8fa6;
      --teal3:#0b5f78;
      --orange:#ff6b00;
      --silver:#c0c0c0;
      --silverLight:#e9edf0;
      --white:#ffffff;
      --text:#0b2a33;
      --shadow:0 6px 18px rgba(0,0,0,.18);
    }
    * { box-sizing: border-box; margin:0; padding:0; }
    body { font-family: 'Inter', Arial, sans-serif; margin:0; background: #eef6f8; }

    .sheet {
      width: 1080px;
      height: auto;
      background: linear-gradient(180deg,var(--teal1),var(--teal2));
      color: var(--white);
      position: relative;
      overflow: hidden;
    }
    /* Watermark overlay */
    .watermark {
      position: absolute;
      inset: 0;
      background-image: url('${logo}');
      background-repeat: repeat;
      background-size: 400px auto; 
      background-position: 0 0;
      opacity: .12;               
      transform: rotate(-25deg);  
      transform-origin: center;
      pointer-events: none;
      z-index: 50;                
    }

    /* ===== HERO ===== */
    .hero { position: relative; height: 560px; z-index: 100; }
    .hero img { width: 100%; height: 560px; object-fit: cover; display: block; z-index: 100; }
    .rooms-chip {
      --chipH: 108px;
      --chipGap: 8px;
      position: absolute; left: 28px; bottom: 24px;
      display: flex; align-items: stretch; gap: 10px;
    }
    .rooms-num {
      width: var(--chipH); height: var(--chipH);
      background: linear-gradient(135deg, var(--teal1) 0%, #56e3f9 50%, var(--teal2) 100%);
      color:#fff; display:flex; align-items:center; justify-content:center;
      border-radius: 14px; font-weight: 900; font-size: 72px;
      box-shadow: var(--shadow);
    }
    .rooms-text { height: var(--chipH); display:flex; flex-direction: column; gap: var(--chipGap); }
    .rooms-text .line {
      flex: 1; display:flex; align-items:center;
      background: linear-gradient(135deg, var(--teal1) 0%, #56e3f9 50%, var(--teal2) 100%);
      color:#fff; padding: 8px 14px; border-radius: 14px;
      font-weight: 800; font-size: 32px; letter-spacing: .3px;
      box-shadow: var(--shadow);
    }
    .price-bubble {
      position: absolute; right: 28px; bottom: 24px;
      background: radial-gradient(circle at center, #f4f5f4 0%, #f4f5f4 45%, #d3d3d2 100%); color:#000;
      padding: 12px 20px; border-radius: 28px;
      font-size: 44px; font-weight:800;
      box-shadow: 0 8px 20px rgba(0,0,0,.3);
      border: 2px solid #d3d3d2;
    }

    /* ===== META (top-right) ===== */
    .meta {
      position: absolute; right: 24px; top: 24px;
      background: linear-gradient(135deg, rgba(255,255,255,.35), rgba(255,255,255,.1));
      border: 1px solid rgba(255,255,255,.55);
      border-radius: 14px;
      padding: 10px 14px;
      box-shadow: 0 6px 18px rgba(0,0,0,.18);
      backdrop-filter: blur(4px);
      color: #062d37;
      font-weight: 800;
    }
    .meta .client { font-size: 20px; }
    .meta .sub { font-size: 16px; opacity: .9; }

    /* ===== PANEL ===== */
    .panel {
      margin-top: -8px;
      background: radial-gradient(circle at center, #43cadf 0%, #4fcde0 40%, #0f7894 100%);
      border-radius: 24px 24px 0 0;
      padding: 24px 28px 12px;
      box-shadow: 0 -4px 12px rgba(0,0,0,.12) inset;
    }
    .brand { font-weight: 800; opacity:.95; margin-bottom: 8px; }
    .brand img { height: 72px; display:block; }
    .title { display:flex; align-items:center; gap: 10px; }
    .title .label { font-size: 64px; font-weight: 900; letter-spacing: 0.5px; }
    .title .label span { color:#fff; text-transform: uppercase; }
    .address { margin-top: 8px; font-size: 28px; opacity: .95; display:flex; align-items:center; gap:10px; }
    .address img.loc { width: 40px; height: 40px; filter: drop-shadow(0 1px 1px rgba(0,0,0,.2)); margin-right: 15px; }

    /* ===== INFO FOOTER (silver block) ===== */
    .info-footer {
      background: transparent;
      border: 2px solid transparent;
      border-radius: 20px;
      padding: 20px;
      margin-top: 18px;
      display: grid;
      grid-template-columns: 1.6fr 1fr;
      gap: 18px;
      color: var(--text);
    }
    .footer-left { display:flex; flex-direction: column; gap: 12px; background: transparent; height: 100%; }
    .footer-title .label { font-size: 68px; font-weight: 900; letter-spacing: .5px; color: #fff; }
    .info-footer .address { color: #fff; }
    .specs-vertical { display:flex; flex-direction: column; gap: 20px; align-items: center; justify-self: end; padding: 20px 0; }
    /* Fallback for browsers without flex-gap support */
    .specs-vertical .spec + .spec { margin-top: 20px; }

    /* ===== SPECS ===== */
    .specs {
      display:grid;
      grid-template-columns: repeat(3, 1fr);
      gap: 14px;
      margin-top: 18px;
    }
    .spec {
      background: radial-gradient(circle at center, #f4f5f4 0%, #f4f5f4 45%, #d3d3d2 100%);
      color:var(--text);
      border: 2px solid #d3d3d2;
      border-radius: 20px;
      padding: 14px 18px;
      display:flex;
      flex-direction: column;
      align-items:center;
      justify-content:center;
      gap: 8px;
      font-weight:800;
      box-shadow: var(--shadow);
      font-size: 22px;
      min-height: 120px;
      width: 230px;
      text-align: center;
      white-space: pre-line;
    }
    .spec img { width: 64px; height: 64px; opacity: .9; }

    /* ===== FEATURES ===== */
    .features {
      margin-top: 18px;
      background: transparent;
      border-radius: 16px;
      padding: 18px;
      flex: 1;
      min-height: 300px;
    }
    .features h3 { margin: 0 0 12px 0; color:#e9feff; font-size: 24px; }
    .benefits-list {
      list-style: none;
      column-count: 2;
      column-gap: 20px;
      padding: 0;
      margin: 0;
      height: 100%;
      column-fill: balance;
    }
    .benefits-list li {
      background: transparent;
      color:#fff;
      padding: 6px 8px;
      border-radius: 0;
      font-weight: 700;
      font-size: 34px;
      display: flex;
      align-items: flex-start;
      gap: 15px;
      box-shadow: none;
      border: none;
      break-inside: avoid;
      -webkit-column-break-inside: avoid;
      column-break-inside: avoid;
      margin-bottom: 4px;
      line-height: 1.2;
    }
    .benefits-list img.chk { width: 34px; height: 34px; margin-top: 4px; flex-shrink: 0; margin-right: 12px; }

    /* ===== PHOTOS ===== */
    .photos {
      margin: 20px 0 0 0;
      display:grid;
      grid-template-columns: repeat(3, 1fr);
      gap: 14px;
    }
    .photos img {
      position: relative;
      z-index: 100;
      width:100%; height: 240px;
      object-fit: cover;
      border-radius: 14px;
      box-shadow: var(--shadow);
      border: 3px solid #fff;
    }

    /* ===== FOOTER ===== */
    .footer {
      display:flex;
      justify-content: space-between;
      align-items:center;
      font-size: 22px;
      color:#d8f4f7;
      padding: 18px 4px;
      margin-top: 12px;
    }
  </style>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700;900&display=swap" rel="stylesheet">
</head>
<body>
  <div class="sheet">
    <div class="watermark"></div>
    <!-- HERO -->
    <div class="hero">
      <img src="${cover}" alt="cover"/>
      ${rooms_chip}
      <div class="price-bubble">${price}</div>
      <div class="meta">
        <!-- <div class="client">${client_name}</div> -->
        <div class="sub">РОП: ${rop}</div>
        <div class="sub">МОП: ${mop}</div>
        <div class="sub">CRM ID: ${crm_id}</div>
      </div>
    </div>

    <!-- PANEL -->
    <div class="panel">
      
      <div class="info-footer">
        <div class="footer-left">
        <div class="brand"><img src="${logo}" alt="logo"/></div>
          ${complex_label}
          <div class="address"><img class="loc" src="${icon_location}" alt="loc"/>   ${address}</div>
          <div class="features">
            <ul class="benefits-list">  ${benefits}</ul>
          </div>
        </div>
        <div class="specs-vertical">
          <div class="spec"><img src="${icon_square}" alt="s"/>S-${area_sqm} м²</div>
          <div class="spec"><img src="${icon_building}" alt="floor"/>${floor}\nЭтаж</div>
          <div class="spec"><img src="${icon_star}" alt="class"/>${housing_class}\nКласс</div>
        </div>
      </div>

      <div class="photos">${photos}</div>

      <div class="footer">
        <!-- <div>${agent_phone}</div> -->
        <div>ivitrina.kz</div>
      </div>
    </div>
  </div>
</body>
</html>
"""


def _to_file_url(path: str) -> str:
    return 'file:///' + os.path.abspath(path).replace('\\', '/')


def _asset_url(filename: str) -> str:
    path = os.path.join('html_source', filename)
    return 'file:///' + os.path.abspath(path).replace('\\', '/')


def _build_html(ci: CollageInput) -> str:
    import logging
    logger = logging.getLogger(__name__)
    
    cover = _to_file_url(ci.photos[0]) if ci.photos else ''
    photos_srcs = ci.photos[1:4] if len(ci.photos) > 1 else []
    photos_tags = ''.join([f'<img src="{_to_file_url(p)}"/>' for p in photos_srcs])

    check_icon = _asset_url('checkmark_icon.png')
    benefits_items = ''.join([
        f'<li><img class="chk" src="{check_icon}" alt="check"/> {html_escape.escape(b)}</li>'
        for b in ci.benefits
    ])

    # Блок с количеством комнат:
    # - рендерим только если значение комнат непустое
    # - для коммерческих объектов ("Коммерческий объект") не показываем вовсе
    rooms_value = (ci.rooms or '').strip()
    object_type = (ci.object_type or '').strip()
    is_commercial = object_type == 'Коммерческий объект'

    if rooms_value and not is_commercial:
        rooms_chip = f'''
      <div class="rooms-chip">
        <div class="rooms-num">{html_escape.escape(rooms_value)}</div>
        <div class="rooms-text">
          <div class="line">комнатная</div>
          <div class="line">квартира</div>
        </div>
      </div>'''
    else:
        rooms_chip = ''

    tpl = Template(COLLAGE_TEMPLATE)
    return tpl.safe_substitute(
        crm_id=ci.crm_id,
        rooms_chip=rooms_chip,
        price=ci.price or '-',
        complex_label=(
            f'<div class="footer-title"><div class="label"><span>{html_escape.escape(ci.complex_name)}</span></div></div>'
            if ci.complex_name else ''
        ),
        address=html_escape.escape(ci.address or '-'),
        area_sqm=html_escape.escape(ci.area_sqm or '-'),
        floor=html_escape.escape(ci.floor or '-'),
        housing_class=html_escape.escape(ci.housing_class or '-'),
        client_name=html_escape.escape(ci.client_name or ''),
        rop=html_escape.escape(ci.rop or ''),
        mop=html_escape.escape(ci.mop or ''),
        agent_phone=html_escape.escape(ci.agent_phone or '8 777 777 7777'),
        cover=cover,
        logo=_asset_url('logo.png'),
        icon_square=_asset_url('square_icon.png'),
        icon_building=_asset_url('building_icon.png'),
        icon_star=_asset_url('star_icon.png'),
        icon_location=_asset_url('location.png'),
        benefits=benefits_items,
        photos=photos_tags,
    )


async def render_collage_to_image(ci: CollageInput) -> tuple[str, str]:
    import logging
    logger = logging.getLogger(__name__)
    
    html_content = _build_html(ci)
    out_dir = os.path.join('data')
    os.makedirs(out_dir, exist_ok=True)
    html_path = os.path.join(out_dir, f"collage_{ci.crm_id}.html")
    with open(html_path, 'w', encoding='utf-8') as f:
        f.write(html_content)

    
    browser = None
    try:
        browser = await launch(
            headless=True,
            args=['--no-sandbox', '--disable-dev-shm-usage', '--disable-gpu'],
            timeout=30000,
            handleSIGINT=False,
            handleSIGTERM=False,
            handleSIGHUP=False
        )
        
        page = await browser.newPage()
        await page.setViewport({'width': 1080, 'height': 1920})
        
        html_url = 'file:///' + os.path.abspath(html_path).replace('\\', '/')
        await page.goto(html_url, waitUntil='networkidle0', timeout=45000)
        # Небольшая пауза, чтобы шрифты/локальные ресурсы гарантированно прогрузились
        await asyncio.sleep(0.4)
        
        image_path = os.path.join(out_dir, f"collage_{ci.crm_id}.png")
        
        sheet = await page.querySelector('.sheet')
        if sheet:
            await sheet.screenshot({'path': image_path, 'type': 'png'})
        else:
            await page.screenshot({'path': image_path, 'type': 'png'})
        
        return image_path, html_path
        
    except Exception as e:
        logger.error(f"Error rendering collage {ci.crm_id}: {e}")
        raise
    finally:
        if browser:
            try:
                await browser.close()
            except Exception:
                pass
