import xml.etree.ElementTree as ET

from tqdm import tqdm

NS = {"tei": "http://www.tei-c.org/ns/1.0"}
roots = []
roots += [ET.parse("Corpus/wikis/pt-BR/WIKa.xml").getroot()]
roots += [ET.parse("Corpus/university domains/UNIa.xml").getroot()]
roots += [ET.parse("Corpus/university domains/UNIb.xml").getroot()]
roots += [ET.parse("Corpus/university domains/UNIc.xml").getroot()]
roots += [ET.parse("Corpus/university domains/UNId.xml").getroot()]
roots += [ET.parse("Corpus/university domains/UNIe.xml").getroot()]
roots += [ET.parse("Corpus/university domains/UNIf.xml").getroot()]
roots += [ET.parse("Corpus/university domains/UNIg.xml").getroot()]

for i in tqdm([1, 10, 100, 1024, 2048]):
    max_file_size = i  # In MB
    char_count = 0
    full_text = ""
    max_count = max_file_size * 1e6

    for root in roots:
        for el in root.findall(".//tei:TEI", NS):
            text_el = el.find(".//tei:text", NS)
            if text_el is None:
                continue

            body = text_el.find(".//tei:body", NS)
            if body is None:
                continue

            for t in body.findall(".//tei:p", NS):
                if not (t.text):
                    continue
                text = "".join(t.text).strip()
                char_count += len(text)
                full_text += text

            if char_count >= max_count:
                break

    with open(f"{max_file_size}mb_file.txt", "w") as f:
        f.write(full_text)
