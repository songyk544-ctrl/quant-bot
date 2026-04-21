import re
from datetime import datetime


def normalize_text(text):
    return re.sub(r"\s+", " ", (text or "")).strip()


def extract_source(title_text, fallback="일반"):
    if " - " in title_text:
        maybe_source = normalize_text(title_text.split(" - ")[-1])
        if 1 < len(maybe_source) <= 12:
            return maybe_source
    return fallback


def event_tags(text):
    text_norm = normalize_text(text).lower()
    rules = {
        "실적": ["실적", "영업이익", "매출", "어닝", "가이던스", "서프라이즈", "흑자전환", "턴어라운드"],
        "수주": ["수주", "계약", "공급", "협약", "납품", "발주", "po", "수출", "수출입"],
        "정책": ["정부", "정책", "규제", "법안", "금리", "관세", "예산", "지원책", "인허가"],
        "수급": ["외국인", "기관", "연기금", "순매수", "공매도", "프로그램매매", "수급"],
        "리스크": ["소송", "리콜", "악재", "부진", "감소", "하향", "충당금", "적자", "불확실성", "지연"],
        "반도체": ["반도체", "메모리", "hbm", "d램", "dram", "낸드", "파운드리", "후공정", "osat"],
        "2차전지": ["2차전지", "이차전지", "배터리", "양극재", "음극재", "전해질", "리튬", "니켈", "전구체"],
        "전력": ["전력", "전력기기", "변압기", "배전", "송전", "원전", "원자력", "전선", "스마트그리드"],
        "방산": ["방산", "국방", "미사일", "탄약", "장갑차", "전투기", "k9", "k2", "함정"],
        "바이오": ["바이오", "제약", "임상", "신약", "adc", "항체", "허가", "fda", "기술수출"],
        "ai": ["ai", "인공지능", "데이터센터", "gpu", "npu", "온디바이스", "llm"],
        "자동차": ["자동차", "완성차", "전기차", "ev", "하이브리드", "자율주행", "부품사"],
        "조선": ["조선", "선박", "lng선", "해양플랜트", "수주잔고"],
        "게임/콘텐츠": ["게임", "콘텐츠", "흥행", "신작", "퍼블리싱", "드라마", "엔터"],
    }
    tags = []
    for tag, keywords in rules.items():
        if any(k in text_norm for k in keywords):
            tags.append(tag)
    return tags


def title_signature(title_text):
    normalized = re.sub(r"[^0-9A-Za-z가-힣 ]+", " ", title_text.lower())
    tokens = [t for t in normalized.split() if len(t) > 1]
    return set(tokens[:12])


def is_similar_title(sig, signature_list, threshold=0.75):
    for other in signature_list:
        if not sig or not other:
            continue
        inter = len(sig & other)
        union = len(sig | other)
        if union > 0 and (inter / union) >= threshold:
            return True
    return False


def source_weight(source):
    weights = {
        "연합뉴스": 1.0,
        "뉴시스": 0.95,
        "이데일리": 0.9,
        "매일경제": 0.9,
        "한국경제": 0.9,
        "머니투데이": 0.88,
        "서울경제": 0.88,
        "일반": 0.82,
    }
    return weights.get(source, 0.84)


def score_news_candidate(candidate, include_relevance=False):
    now = datetime.now()
    news_dt = candidate.get("dt")
    age_score = 0.7
    if news_dt is not None:
        diff_h = max(0.0, (now - news_dt).total_seconds() / 3600)
        if diff_h <= 6:
            age_score = 1.1
        elif diff_h <= 24:
            age_score = 1.0
        elif diff_h <= 48:
            age_score = 0.85
        else:
            age_score = 0.6
    tag_bonus = min(0.35, 0.12 * len(candidate.get("tags", [])))
    text_quality = 0.08 if len(candidate.get("desc", "")) >= 20 else 0.0
    score = source_weight(candidate.get("source", "일반")) + age_score + tag_bonus + text_quality
    if include_relevance:
        score += 0.18 if candidate.get("is_relevant", False) else -0.08
    return score
