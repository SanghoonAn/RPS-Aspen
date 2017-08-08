# RPS-Aspen-Application Reports and Stored Procedures.
Richmond Public Schools Aspen application reports and procedures.

<h3>#Login</h3><br>
Use the following link to access the portal.
https://rpsanywhere.richmond.k12.va.us/Citrix/XenApp/auth/login.aspx
It will promot the user to install the Citrix Client.

Login using the following credentials.<br>
l:  rmcnamee.contractor<br>
p:  BigJack#8

Once logged in, click on the Aspen SIS link to open the browser. <br><br>
<b>#Important#  The default page opened bu clicking the link is the RPS Aspen PRODUCTION site.  Production site can be reached without the Citrix vpn.  Make sure not to confuse the production site with the dev/test environment.</b>
 
So, once IE opens up and gives you a login for Aspen, replace the address with http://app-aspen-dev/aspenrep.<br>
Below is my personal login to the aspenRep site. <br>
L: san<br>
P: password01

<h3>#Location of Custom Codes.</h3>
All Reports are accessible from the Distric view under 
<ul style="line-height:80%">
  <li>Tools->Reports</li>
  <li>Tools->Procedures</li>
  <li>Tools->Imports</li>
  <li>Tools->Exports</li>
</ul>

<h3>Updating/Creating Reports and Procedures</h3> 
From one of the side tabs listed above, click on the entity to update or click "Options" then select "Add".  Fille out the Name and the ID. Either Update or upload the Source Code and Input definition.  For Reports, also update or upload the format definition.

<h3>Java Development</h3>  
Recommended development tool is Eclipse Mars.  It is free to download at https://eclipse.org/downloads/ 
Once the Eclipse is installed, download and import the Aspen.war file.


