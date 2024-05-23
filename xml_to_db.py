##########################
## Script purpose: Import archive (xml-files) into DB
## Important to know:
##
## Author
## name: Manuel Frick
## e-mail: mf@awp.ch
##
## Python-Version: 3.11
##########################

# Load functions
from awptools import utils
from glob import glob
from xml.dom import minidom
import os
import regex
import datetime
import tiktoken  # used for token counter
import logging

# Set encoding for token count
encoding = tiktoken.get_encoding("cl100k_base")  # cl100k_base is used in GPT4, GPT3.5 etc.

# Init global variables
wires_permitted = ("P", "K", "N")
alert_receivers = "mf@awp.ch"
project_dir = os.getcwd()
input_dir = os.path.join(project_dir, "_input")

# Prepare logging
log_dir = os.path.join(project_dir, 'logs')
if not os.path.exists(log_dir): os.mkdir(log_dir)
log_name = os.path.join(log_dir, datetime.datetime.now().strftime("%Y%m%d_%H%M%S") + '_xml_to_db.log')
logging.basicConfig(filename=log_name,
                    level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Get list of input files
files = []
pattern = "*.xml"
for directory, _, _ in os.walk(input_dir):
    files.extend(glob(os.path.join(directory, pattern)))
logging.info("Number of files: " + str((len(files))) + "\n")

# Establish DB connection
archive_db = utils.connect_db("archive")
archive_cursor = archive_db.cursor()



# Iterate over files
for file in files:

    try:

        ##############
        # Extraction #
        ##############

        # Parse XML
        dom = minidom.parse(file)

        # Extract title
        title = dom.getElementsByTagName("HeadLine")[0]._get_firstChild().nodeValue
        title = title.replace("<<", "\"").replace(">>", "\"")

        # Extract byline
        byline = None
        for element in dom.getElementsByTagName("NewsLineType"):
            if element.getAttribute("FormalName") == "ByLine":
                node = element.nextSibling._get_firstChild()
                if node is not None:
                    byline = node.nodeValue

        # Extract text (tables will be ignored)
        text = ""
        table_contained = False
        paragraph_count = int(0)
        first_paragraph_char_count = int(0)
        for node in dom.getElementsByTagName("body.content")[0]._get_childNodes():
            # nodeType must be "ELEMENT_NODE" ("\n" counts as node in minidom)
            if node.nodeType == 1:
                if node.hasChildNodes():
                    node_text = node._get_firstChild().nodeValue
                    if node.tagName == "p" or node.tagName == "h3":
                        # Paragraph
                        text += node_text
                        paragraph_count += 1
                        if first_paragraph_char_count == 0:
                            first_paragraph_char_count = len(text)
                    elif node.tagName == "pre":
                        table_contained = True
                elif node.tagName == "p":
                    # Line break
                    if not text.endswith("\n\n"):
                        text += "\n\n"
        text = text.replace("<<", "\"").replace(">>", "\"")
        if ("[[" in text) and ("]]" in text):
            text = regex.sub('\[\[.+\]\]', '', text, flags=regex.DOTALL)  # Remove table
            table_contained = True

        # Get authors from extracted text
        authors = ""
        last_paragraph = text.split("\n\n")[-1]
        if len(last_paragraph) <= 22:  # Exceeding length means probably missing author line
            authors = last_paragraph.replace("/", " ").strip()

        # Detect AWP copyright
        copyright_awp = False
        agencies = regex.search(r'\s\([^)]*\)', text)
        if agencies:
            agencies = agencies.group().upper().strip()
            if agencies == "(AWP)" or agencies == "(AWP INTERNATIONAL)":
                copyright_awp = True

        # Assemble complete text incl. title and byline
        text_complete = title + "\n\n"
        if byline is not None:
            text_complete += byline + "\n\n"
        text_complete += text

        # Extract date and time of publishing
        publish_timestamp = datetime.datetime.strptime(
            dom.getElementsByTagName("FirstCreated")[0]._get_firstChild().nodeValue[0:15],
            "%Y%m%dT%H%M%S")
        publish_date = datetime.datetime.strftime(publish_timestamp, "%Y-%m-%d")
        publish_time = datetime.datetime.strftime(publish_timestamp, "%H:%M:%S")

        # Extract language
        language = dom.getElementsByTagName("Language")[0].getAttribute("FormalName")

        # Extract companies, wire, subject codes
        companies_name = list()
        companies_id = list()
        subjects = list()
        industries = list()
        countries = list()
        wires = list()
        for element in dom.getElementsByTagName("Property"):
            attr = element.getAttribute("FormalName")
            if attr == "FullName":
                companies_name.append(element.getAttributeNode("Value").nodeValue)
            elif attr == "Company":
                companies_id.append(element.getAttributeNode("Value").nodeValue)
            elif attr == "Wire":
                wires.append(element.getAttributeNode("Value").nodeValue)
            elif attr == "Subject":
                subjects.append(element.getAttributeNode("Value").nodeValue)
            elif attr == "Industry":
                industries.append(element.getAttributeNode("Value").nodeValue)
            elif attr == "Country":
                countries.append(element.getAttributeNode("Value").nodeValue)

        # print("-----------------")
        # print(file)
        # print("Title: " + title)
        # print("Byline: " + str(byline))
        # print("Companies (Name): " + str(companies_name))
        # print("Companies (BW2 ID): " + str(companies_id))
        # print("Subjects: " + str(subjects))
        # print("Industries: " + str(industries))
        # print("Countries: " + str(countries))
        # print("Wires: " + str(wires))
        # print("Authors: " + authors)
        # print("Publish date: " + publish_date)
        # print("Publish time: " + publish_time)
        # print("Language: " + language)
        # print(text)


        #############
        # Measuring #
        #############

        # Word count
        word_count = len(regex.findall(r'\w+', text_complete))

        # Token count
        token_count = len(encoding.encode(text_complete))

        # print("Number of words: " + str(word_count))
        # print("Number of tokens: " + str(token_count))


        #############
        # Filtering #
        #############

        save_to_db = True

        # if not copyright_awp:
        #     save_to_db = False  # No text from sda, dpa...

        # Repetition (Wdh / répétition)
        repetition = False
        if regex.match("^[*]*(Wdh|WDH)\\s?\\d?[/:]?\\s?", title):
            repetition = True
        if byline:
            if byline == "(répétition)":
                repetition = True
        if repetition:
            save_to_db = False

        # Flash
        if title.startswith("***"):
            save_to_db = False

        # Table
        if ("TABELLE" in title.upper()) or ("TABLEAU" in title.upper()):
            save_to_db = False
        if table_contained and paragraph_count == 1 and first_paragraph_char_count < 160:
            save_to_db = False  # Only one very short paragraph
        if table_contained and word_count < 40:
            save_to_db = False  # Text besides table too short
        if ("INNERER WERT" in title.upper()) or ("INNERE WERTE" in title.upper()):
            save_to_db = False

        # Impressum, Abkürzungen...
        if "SER" in subjects:
            save_to_db = False
        if ("Abkürzungen" in title) or ("Abréviations" in title):
            save_to_db = False

        # Terminvorschau...
        if "CAL" in subjects:
            save_to_db = False

        # Wire
        one_wire_ok = False
        for wire in wires:
            if wire in wires_permitted: # Only P, K, N
                one_wire_ok = True
        if not one_wire_ok:
            save_to_db = False


        ##############
        # Save to DB #
        ##############

        if save_to_db:

            # Prep data for SQL
            title = title.replace("'", "''")
            text = text.replace("'", "''")
            text_complete = text_complete.replace("'", "''")
            authors = authors.replace("'", "''")
            wires = " ".join(wires)
            subjects = " ".join(subjects)
            industries = " ".join(industries)
            countries = " ".join(countries)
            companies_id = " ".join(companies_id)
            companies_name = " | ".join(companies_name).replace("'", "''")

            sql_stmt = f'INSERT INTO archive.archive_llm ' \
                       f'(publish_date, publish_time, title, text_redsys, ' \
                       f'text_incl_title_byline, copyright_awp, word_count, token_count_openai, ' \
                       f'wires, language, authors, ' \
                       f'subjects, industries, countries, ' \
                       f'companies_id_redsys, companies_name) ' \
                       f'VALUES (\'{publish_date}\', \'{publish_time}\', \'{title}\', \'{text}\', ' \
                       f'\'{text_complete}\', {int(copyright_awp)}, {word_count}, {token_count}, ' \
                       f'\'{wires}\', \'{language}\', \'{authors}\', ' \
                       f'\'{subjects}\', \'{industries}\', \'{countries}\', ' \
                       f'\'{companies_id}\', \'{companies_name}\');'
            archive_cursor.execute(sql_stmt)
            archive_db.commit()


        #######################
        # Move processed file #
        #######################

        if save_to_db:
            utils.move_file(os.path.join(input_dir, file), "processed", project_dir)
        else:
            utils.move_file(os.path.join(input_dir, file), "ignored", project_dir)


    except Exception as err:

        # # Mail alert
        # mail_subj = "Error: Archiv-Import-Prozess gescheitert"
        # mail_body = ('Der Archiv-Import-Prozess wurde aufgrund eines unerwarteten Fehlers beendet.' +
        #              '\n\nEs gab folgende Fehlermeldung:\n' + str(err) + '\n\nDie Datei  ' +
        #              file + ' wird in den Ordner ' + project_dir +
        #              '\\_erroneous verschoben.\n\nStaubige Grüsse\n\nAWP Robot')
        # utils.send_notification(mail_subj, mail_body, alert_receivers)

        # Log error
        logging.exception(str(err) + "\n" + "Datei " + file + "\n", exc_info=False)

        # Move file
        utils.move_file(os.path.join(input_dir, file), "erroneous", project_dir)


############
# Clean up #
############

# Close DB connection
archive_cursor.close()
archive_db.close()

# Delete empty directories in input folder
folders = list(os.walk(input_dir))[1:]
for folder in folders:
    if not folder[2]:
        os.rmdir(folder[0])

logging.info("Process successfully terminated")
