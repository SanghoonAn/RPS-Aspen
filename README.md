# RPS-Aspen-Application Reports and Stored Procedures.
Richmond Public Schools Aspen application reports and procedures.

# Environment:

<h4>RPS Production</h4>
https://aspen.richmond.k12.va.us/aspen/logon.do

<h4>RPS Development Environment</h4>
Use the following link to access the RPS Anywhere portal.
https://rpsanywhere.richmond.k12.va.us/Citrix/XenApp/auth/login.aspx
It will promot the user to install the Citrix Client.

Login using the following credentials.<br>
l:  rmcnamee.contractor<br>
p:  BigJack#8

Once logged in, click on the Aspen SIS link to open the browser. <br><br>
<b>IMPORTANT: The default page opened bu clicking the link is the RPS Aspen PRODUCTION site.  Production site can be reached without the Citrix vpn.  Make sure not to confuse the production site with the dev/test environment.</b>
 
So, once IE opens up and gives you a login for Aspen, replace the address with http://app-aspen-dev/aspenrep.<br>
Below is my personal login to the aspenRep site. <br>
L: san<br>
P: password01

# Location of Custom Codes 
All Reports are accessible from the Distric view under 
<ul style="line-height:80%">
  <li>Tools->Reports</li>
  <li>Tools->Procedures</li>
  <li>Tools->Imports</li>
  <li>Tools->Exports</li>
</ul>

# Updating/Creating Reports and Procedures 
<h4> Details</h4>
From one of the side tabs listed above, click on the entity to update or click "Options" then select "Add".  Fille out the Name and the ID. Either Update or upload the Source Code and Input definition.  For Reports, also update or upload the format definition.

<h4> Navigation tab</h4>
Navigation tab determines where the report or stored procedure can be found and used.  Every page in Aspen has a navkey.  It refers to the main object type being displayed on that page.  For example the Student top-tab, Details side tab will have URL of https://aspen.richmond.k12.va.us/aspen/personAddressDetail.do?navkey=student.std.list.detail, which ends with navkey=student.std.list.detail.   This very key can be copied and pasted into the Navigations tab or a report or procedure to make them available from this particular view. Most pages are available from multiple views.  For example, the student details page is available from District, school, staff and student viwes.  So when creating a navigation row one must define the navkey and select the views in which this report or procedure should be available.   

<h4>Sub-Reports</h4>
If your report uses one or more sub reports, each sub reports must be created as a separate report entity.  Sub reports may or may not need a source code but required to have the format definition.  ID of the report must match the ID referred in the source code of the main report.



# Java Development  
Recommended development tool is Eclipse Mars.  It is free to download at https://eclipse.org/downloads/ 
Once the Eclipse is installed, download and import the Aspen.war file.
New Objects should be placed and developed under aspen-> Java Resources -> src ->aspen.
Depending on the intended use of the object, it needs to inherit a proper parent object.
Reports extend "ReportJavaSourceNet" store procedures should extend "ProcedureJavaSource" in most cases.  


# Format Definitions Development 
The format definitions are xml files generated from Ireport.  Version supported by Aspen are 1.0.2, 3.0.1 and 5.5.0.  
<b>IMPORTANT: On the report details page, theres a field for Report Engine Version.  Version number on this field must match that of the report editor used to create the format definition file.  

