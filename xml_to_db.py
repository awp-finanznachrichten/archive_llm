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

# Set encoding for token count
encoding = tiktoken.get_encoding("cl100k_base")  # cl100k_base is used in GPT4, GPT3.5 etc.

# Init global variables
wires_permitted = ("P", "K", "N")
alert_receivers = "mf@awp.ch"
project_dir = os.getcwd()
input_dir = os.path.join(project_dir, "_input")

# Get list of input files
files = []
pattern = "*.xml"
for directory, _, _ in os.walk(input_dir):
    files.extend(glob(os.path.join(directory, pattern)))
print(len(files))

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

        # Extract byline
        byline = None
        for element in dom.getElementsByTagName("NewsLineType"):
            if element.getAttribute("FormalName") == "ByLine":
                node = element.nextSibling._get_firstChild()
                if node is not None:
                    byline = node.nodeValue

        # Detect repetition (Wdh)
        repetition = False
        if regex.match("^[*]*(Wdh|WDH)\\s?\\d?[/:]?\\s?", title):
            repetition = True

        # Extract text (tables will be ignored)
        text = ""
        for node in dom.getElementsByTagName("body.content")[0]._get_childNodes():
            # nodeType must be "ELEMENT_NODE" ("\n" counts as node in minidom)
            if node.nodeType == 1:
                if node.hasChildNodes():
                    node_text = node._get_firstChild().nodeValue
                    if node.tagName == "p" or node.tagName == "h3":
                        # Paragraph
                        text += node_text
                elif node.tagName == "p":
                    # Line break
                    text += "\n\n"

        # Get authors from extracted text
        authors = text.split("\n\n")[-1].replace("/", " ").strip()

        # Detect AWP copyright
        copyright_awp = False
        agencies = regex.search(r'\(\w+\)', text).group()
        if agencies == "(awp)" or agencies == "(awp international)":
            copyright_awp = True

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
        wire = ""
        for element in dom.getElementsByTagName("Property"):
            attr = element.getAttribute("FormalName")
            if attr == "FullName":
                companies_name.append(element.getAttributeNode("Value").nodeValue)
            elif attr == "Company":
                companies_id.append(element.getAttributeNode("Value").nodeValue)
            elif attr == "Wire":
                wire = element.getAttributeNode("Value").nodeValue
            elif attr == "Subject":
                subjects.append(element.getAttributeNode("Value").nodeValue)
            elif attr == "Industry":
                industries.append(element.getAttributeNode("Value").nodeValue)
            elif attr == "Country":
                countries.append(element.getAttributeNode("Value").nodeValue)

        print("-----------------")
        print(file)
        print("Title: " + title)
        print("Wdh: " + str(repetition))
        print("Byline: " + str(byline))
        print("Companies (Name): " + str(companies_name))
        print("Companies (BW2 ID): " + str(companies_id))
        print("Subjects: " + str(subjects))
        print("Industries: " + str(industries))
        print("Countries: " + str(countries))
        print("Wire: " + str(wire))
        print("Authors: " + authors)
        print("Publish date: " + publish_date)
        print("Publish time: " + publish_time)
        print("Language: " + language)
        print(text)


        #############
        # Filtering #
        #############

        save_to_db = True

        if not copyright_awp:
            save_to_db = False  # No text from sda, dpa...

        if repetition:
            save_to_db = False  # No Wdh

        if title.startswith("***"):  # No flashes
            save_to_db = False

        if "SER" in subjects:
            save_to_db = False  # No Impressum, Abkürzungen...

        if "CAL" in subjects:
            save_to_db = False  # No Terminvorschau...

        if not wire in wires_permitted:
            save_to_db = False  # Only P, K, N


        #############
        # Measuring #
        #############

        # Word count
        word_count = len(regex.findall(r'\w+', text))

        # Token count
        token_count = len(encoding.encode(text))

        print("Number of words: " + str(word_count))
        print("Number of tokens: " + str(token_count))


        ##############
        # Save to DB #
        ##############

        sql_stmt = f'INSERT INTO archive.archive_llm ' \
                   f'(publish_date, publish_time, title, text, authors, ' \
                   f'copyright_awp, word_count, token_count_openai, wire, language, ' \
                   f'subjects, industries, countries, ' \
                   f'companies_id_BW2, companies_name) ' \
                   f'VALUES (\'{publish_date}\', \'{publish_time}\', \'{title}\', \'{text}\', \'{authors}\', ' \
                   f'{int(copyright_awp)}, {word_count}, {token_count}, \'{wire}\', \'{language}\', ' \
                   f'\'{" ".join(subjects)}\', \'{" ".join(industries)}\', \'{" ".join(countries)}\', ' \
                   f'\'{" ".join(companies_id)}\', \'{" ".join(companies_name)}\');'
        archive_cursor.execute(sql_stmt)
        archive_db.commit()


    except Exception as err:

        # Mail alert
        mail_subj = "Error: Archiv-Import-Prozess gescheitert"
        mail_body = ('Der Archiv-Import-Prozess wurde aufgrund eines unerwarteten Fehlers beendet.' +
                     '\n\nEs gab folgende Fehlermeldung:\n' + str(err) + '\n\nDie Datei  ' +
                     file + ' wird in den Ordner ' + project_dir +
                     '\\_erroneous verschoben.\n\nStaubige Grüsse\n\nAWP Robot')
        utils.send_notification(mail_subj, mail_body, alert_receivers)

        # Move file
        utils.move_file(os.path.join(input_dir, file), "erroneous", project_dir)


############
# Clean up #
############

# Close DB connection
archive_cursor.close()
archive_db.close()
