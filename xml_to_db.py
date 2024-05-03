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

# Init global vars
alert_receivers = "mf@awp.ch"
project_dir = os.getcwd()
input_dir = os.path.join(project_dir, "_input")

# Get list of input files
files = []
pattern = "*.xml"
for directory, _, _ in os.walk(input_dir):
    files.extend(glob(os.path.join(directory, pattern)))
print(len(files))


# Iterate over files
for file in files:

    try:
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
        if regex.match("^[*]*(Wdh|WDH)\\s?\\d?[/:]?\\s?", title):
            repetition = True

        # Extract text
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

        # Extract companies, wire, subject codes
        companies_name = list()
        companies_id = list()
        subjects = list()
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

        print("-----------------")
        print(file)
        print("Title: " + title)
        print("Wdh: " + str(repetition))
        print("Byline: " + str(byline))
        print("Companies (Name): " + str(companies_name))
        print("Companies (BW2 ID): " + str(companies_id))
        print("Subjects: " + str(subjects))
        print("Wire: " + str(wire))
        print(text)

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
