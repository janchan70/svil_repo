import xml.etree.ElementTree as ET
data='''<?xml version="1.0" encoding="UTF-8"?>
<metadata>
<food>
    <item name="breakfast">Idly</item>
    <price>$2.5</price>
    <description>
   Two idly's with chutney
   </description>
    <calories>553</calories>
</food>
</metadata>
'''
myroot = ET.fromstring(data)
print(myroot)
print(myroot.tag)
print(myroot.find(".//calories").text)
print(myroot.find(".//description").text)
print(myroot.find(".//item").attrib)
print(myroot.find(".//item").attrib['name'])

