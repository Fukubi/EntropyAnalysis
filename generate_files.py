import xml.etree.ElementTree as ET

from tqdm import tqdm

NS = {"tei": "http://www.tei-c.org/ns/1.0"}
roots = []
roots += ["Corpus/datasets and other corpora/pt-BR/DATa.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATaa.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATab.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATac.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATad.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATae.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATaf.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATag.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATah.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATai.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATaj.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATak.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATal.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATam.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATan.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATao.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATap.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATaq.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATar.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATas.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATat.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATau.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATav.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATaw.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATax.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATay.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATaz.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATb.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATba.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATbb.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATbc.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATbd.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATbe.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATbf.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATbg.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATbh.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATbi.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATbj.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATbk.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATbl.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATbm.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATbn.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATbo.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATbp.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATbq.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATbr.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATbs.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATbt.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATbu.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATbv.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATbw.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATbx.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATby.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATbz.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATc.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATca.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATcb.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATcc.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATcd.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATce.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATcf.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATcg.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATch.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATci.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATcj.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATck.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATcl.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATd.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATe.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATf.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATg.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATh.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATi.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATj.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATk.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATl.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATm.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATn.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATo.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATp.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATq.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATr.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATs.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATt.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATu.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATv.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATw.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATx.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATy.xml"]
roots += ["Corpus/datasets and other corpora/pt-BR/DATz.xml"]
roots += ["Corpus/university domains/UNIa.xml"]
roots += ["Corpus/university domains/UNIb.xml"]
roots += ["Corpus/university domains/UNIc.xml"]
roots += ["Corpus/university domains/UNId.xml"]
roots += ["Corpus/university domains/UNIe.xml"]
roots += ["Corpus/university domains/UNIf.xml"]
roots += ["Corpus/university domains/UNIg.xml"]
roots += ["Corpus/wikis/pt-BR/WIKa.xml"]

for i in tqdm([1, 10, 100, 1024, 2048]):
    max_file_size = i  # In MB
    char_count = 0
    full_text = ""
    max_count = max_file_size * 1e6

    for f_p in roots:
        root = ET.parse(f_p).getroot()
        for el in root.findall(".//tei:TEI", NS):
            text_el = el.find(".//tei:text", NS)
            if text_el is None:
                continue

            body = text_el.find(".//tei:body", NS)
            if body is None:
                continue

            """for t in body.findall(".//tei:p", NS):
                if not (t.text):
                    continue
                text = "".join(t.text).strip()
                char_count += len(text)
                full_text += text"""
            text = " ".join(body.itertext()).strip()
            char_count += len(text)
            full_text += text

            if char_count >= max_count:
                break

    with open(f"{max_file_size}mb_file.txt", "w") as f:
        f.write(full_text)
