import static com.follett.fsc.core.k12.business.ModelProperty.PATH_DELIMITER;                                                 
                                                                                                                              
import java.io.InputStreamReader;                                                                                             
import org.apache.commons.httpclient.HttpClient;                                                                              
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;                                                      
import org.apache.ojb.broker.query.Criteria;                                                                                  
import org.apache.ojb.broker.query.QueryByCriteria;                                                                           
                                                                                                                              
import com.follett.fsc.core.k12.beans.QueryIterator;                                                                          
import com.follett.fsc.core.k12.tools.reports.ReportDataGrid;                                                                 
import com.follett.fsc.core.k12.tools.reports.ReportJavaSourceNet;                                                            
import com.follett.fsc.core.k12.web.UserDataContainer;                                                                        
import com.x2dev.sis.model.beans.*;                                                                                           
import com.follett.fsc.core.k12.beans.*;                                                                                      
import com.x2dev.utils.types.PlainDate;                                                                                       
import dori.jasper.engine.JRDataSource;                                                                                       
import java.io.Reader;                                                                                                        
import java.io.StringReader;                                                                                                  
import java.io.StringWriter;                                                                                                  
import java.io.Writer;                                                                                                        
import java.net.URLEncoder;                                                                                                   
                                                                                                                              
import javax.xml.parsers.DocumentBuilder;                                                                                     
import javax.xml.parsers.DocumentBuilderFactory;                                                                              
import javax.xml.xpath.XPath;                                                                                                 
import javax.xml.xpath.XPathConstants;                                                                                        
import javax.xml.xpath.XPathExpression;                                                                                       
import javax.xml.xpath.XPathExpressionException;                                                                              
import javax.xml.xpath.XPathFactory;                                                                                          
                                                                                                                              
import org.apache.commons.httpclient.methods.GetMethod;                                                                       
import org.apache.commons.lang.StringUtils;                                                                                   
import org.w3c.dom.Document;                                                                                                  
import org.w3c.dom.NodeList;                                                                                                  
import org.xml.sax.InputSource;                                                                                               
                                                                                                                              
                                                                                                                              
                                                                                                                              
                                                                                                                              
public class Attendance_intervention_visit_plan_report extends ReportJavaSourceNet {                                          
	                                                                                                                             
	                                                                                                                             
	/**                                                                                                                          
	 *                                                                                                                           
	 */                                                                                                                          
    private static final String GEOCODE_REQUEST_URL = "http://maps.googleapis.com/maps/api/geocode/xml?sensor=false&";        
    private static HttpClient httpClient = new HttpClient(new MultiThreadedHttpConnectionManager());                          
	private static final long serialVersionUID = 1L;                                                                             
	private static final String			STUDENT				="student";                                                                         
	private static final String			ABSENCES			="absences";                                                                        
    private static final String	LONGITUDE                 = "lon";                                                            
    private static final String	LATITUDE                       = "lat";                                                       
    private static final String	LONGITUDESTR                 = "lonstr";                                                      
    private static final String	LATITUDESTR                       = "latstr";                                                 
    private static final String	MARKERLABEL                       = "mLabel";                                                 
    public static final String AIP = "Attendance Intervention Plan";                                                          
    /**                                                                                                                       
     * Name for the enumerated "selection" report parameter. The value is an Integer.                                         
     */                                                                                                                       
    public static final String QUERY_BY_PARAM = "queryBy";                                                                    
                                                                                                                              
    /**                                                                                                                       
     * Name for the enumerated "sort" report parameter. The value is an Integer.                                              
     */                                                                                                                       
    public static final String SORT_PARAM = "sort";                                                                           
                                                                                                                              
                                                                                                                              
                                                                                                                              
	                                                                                                                             
	private PlainDate 					districtStartDate;                                                                                    
	private char mapLabel;                                                                                                       
    private float lon;                                                                                                        
    private float lat;                                                                                                        
    private String lonstr;                                                                                                    
    private String latstr;                                                                                                    
                                                                                                                              
	SisStudent m_currentStudent;                                                                                                 
	ReportDataGrid reportGrid = new ReportDataGrid();                                                                            
	                                                                                                                             
	@Override                                                                                                                    
	protected JRDataSource gatherData()                                                                                          
	{                                                                                                                            
		                                                                                                                            
        Criteria criteria = new Criteria();                                                                                   
        mapLabel = 'A';                                                                                                       
   /*                                                                                                                         
          int queryBy = ((Integer) getParameter(QUERY_BY_PARAM)).intValue();                                                  
        switch (queryBy)                                                                                                      
        {                                                                                                                     
            case 0: // Current selection                                                                                      
                criteria = getCurrentCriteria();                                                                              
               break;                                                                                                         
                                                                                                                              
            default:                                                                                                          
                // No additional criteria (this is the case for "All")                                                        
                break;                                                                                                        
        }  */                                                                                                                 
        criteria = getCurrentCriteria();                                                                                      
       QueryByCriteria query = createQueryByCriteria(Student.class, criteria);                                                
		                                                                                                                            
		if (m_currentStudent != null){                                                                                              
			ProcessStudent(m_currentStudent);                                                                                          
		}                                                                                                                           
		else                                                                                                                        
		{ 	                                                                                                                         
			QueryIterator iterator = getBroker().getIteratorByQuery(query);                                                            
		//QueryByCriteria query = createQueryByCriteria(SisStudent.class, criteria);                                                
		 try                                                                                                                        
	        {                                                                                                                    
	                while (iterator.hasNext())                                                                                   
	            {                                                                                                                
	                SisStudent student = (SisStudent) iterator.next();                                                           
	                ProcessStudent(student);                                                                                     
	            }                                                                                                                
	                                                                                                                             
	        }                                                                                                                    
		 finally                                                                                                                    
	        {                                                                                                                    
	            iterator.close();                                                                                                
	        }                                                                                                                    
		}                                                                                                                           
		return reportGrid;                                                                                                          
	}                                                                                                                            
    /**                                                                                                                       
  * @see com.follett.fsc.core.k12.tools.ToolJavaSource#saveState(com.follett.fsc.core.k12.web.UserDataContainer)              
  */                                                                                                                          
 @Override                                                                                                                    
 protected void saveState(UserDataContainer userData)                                                                         
 {                                                                                                                            
     /*                                                                                                                       
      * If we're in the context of a single student, print the report for just that student                                   
      */                                                                                                                      
     m_currentStudent = (SisStudent) userData.getCurrentRecord(SisStudent.class);                                             
 }                                                                                                                            
                                                                                                                              
                                                                                                                              
protected void ProcessStudent(SisStudent student)                                                                             
{                                                                                                                             
                                                                                                                              
	Criteria criteria = new Criteria();                                                                                          
    criteria.addEqualTo(StudentEdPlan.REL_EXTENDED_DATA_DICTIONARY + PATH_DELIMITER + ExtendedDataDictionary.COL_NAME  , AIP);
	criteria.addEqualTo(StudentEdPlan.COL_STUDENT_OID ,  student.getOid());                                                      
	criteria.addNotEqualTo(StudentEdPlan.COL_STATUS_CODE, 4);                                                                    
	criteria.addNotEqualTo(StudentEdPlan.COL_STATUS_CODE, 5);                                                                    
	 QueryByCriteria query = new QueryByCriteria(StudentEdPlan.class, criteria);                                                 
	                                                                                                                             
	 QueryIterator activeEdPlans = getBroker().getIteratorByQuery(query) ;                                                       
	  lon =(float) 0;                                                                                                            
      lat = (float) 0;                                                                                                        
      lonstr = null;                                                                                                          
      latstr = null;                                                                                                          
	                                                                                                                             
	                                                                                                                             
	 SisAddress address= student.getPerson().getPhysicalAddress();                                                               
	                                                                                                                             
	  if (address != null)                                                                                                       
	  {                                                                                                                          
		  String addressString = address.getAddressLine01();                                                                        
		  if (address.getAddressLine02()!=null)                                                                                     
		  {                                                                                                                         
			  addressString = addressString.concat(" ".concat(address.getAddressLine02()));                                            
		  }                                                                                                                         
		  if (address.getAddressLine03()!=null)                                                                                     
		  {                                                                                                                         
			  addressString = addressString.concat(" ".concat(address.getAddressLine03()));                                            
		  }                                                                                                                         
		                                                                                                                            
		  getLongitudeLatitude(addressString );                                                                                     
	  }                                                                                                                          
	                                                                                                                             
/*	  while (activeEdPlans.hasNext())                                                                                          
	 {                                                                                                                           
	StudentEdPlan	 edPlan = ( StudentEdPlan) activeEdPlans.next();  */                                                           
	reportGrid.append();                                                                                                         
    reportGrid.set(STUDENT, student);                                                                                         
   // reportGrid.set(ABSENCES, getAbsences(student,edPlan));                                                                  
    reportGrid.set(LONGITUDE, lon );                                                                                          
    reportGrid.set(LATITUDE, lat );                                                                                           
    reportGrid.set(LONGITUDESTR, lonstr );                                                                                    
    reportGrid.set(LATITUDESTR, latstr );                                                                                     
    reportGrid.set(MARKERLABEL,String.valueOf( mapLabel) );                                                                   
    reportGrid.beforeTop();                                                                                                   
                                                                                                                              
    mapLabel = (char) (mapLabel+1);                                                                                           
	// }                                                                                                                         
}                                                                                                                             
                                                                                                                              
                                                                                                                              
                                                                                                                              
                                                                                                                              
protected int getAbsences( SisStudent student,StudentEdPlan m_edPlan)                                                         
{                                                                                                                             
	int Absences =0;                                                                                                             
	PlainDate StartDate = null;                                                                                                  
	Criteria EdCriteria = new Criteria();                                                                                        
	EdCriteria.addEqualTo(StudentEdPlan.REL_EXTENDED_DATA_DICTIONARY + PATH_DELIMITER + ExtendedDataDictionary.COL_NAME  , AIP); 
	EdCriteria.addNotEqualTo(StudentEdPlan.COL_STATUS_CODE, 4);                                                                  
	EdCriteria.addNotEqualTo(StudentEdPlan.COL_STATUS_CODE, 5);                                                                  
	EdCriteria.addNotEqualTo(StudentEdPlan.COL_OID, m_edPlan.getOid());                                                          
	EdCriteria.addEqualTo(StudentEdPlan.COL_STUDENT_OID ,  student.getOid());                                                    
	//EdCriteria.addEqualTo(StudentEdPlan.COL_FIELD_B001,   FIVE_DAY_PLAN );                                                     
    QueryByCriteria query = new QueryByCriteria(StudentEdPlan.class, EdCriteria);                                             
    query.addOrderByDescending(StudentEdPlan.COL_EFFECTIVE_DATE );                                                            
    QueryIterator activeEdPlans = getBroker().getIteratorByQuery(query) ;                                                     
    	                                                                                                                         
    districtStartDate = this.getOrganization().getCurrentContext().getStartDate();                                            
                                                                                                                              
    if (activeEdPlans.hasNext())                                                                                              
    {                                                                                                                         
    	StudentEdPlan edPlan = (StudentEdPlan) activeEdPlans.next();                                                             
    	StartDate = edPlan.getEffectiveDate();                                                                                   
    }                                                                                                                         
    else                                                                                                                      
    {                                                                                                                         
    	StartDate = districtStartDate;                                                                                           
    }                                                                                                                         
                                                                                                                              
                                                                                                                              
    Criteria absentCriteria = new Criteria();                                                                                 
    absentCriteria.addEqualTo(StudentAttendance.COL_STUDENT_OID ,  student.getOid());                                         
    absentCriteria.addEqualTo(StudentAttendance.COL_ABSENT_INDICATOR ,"Y");                                                   
    absentCriteria.addEqualTo(StudentAttendance.COL_EXCUSED_INDICATOR ,"N");                                                  
    absentCriteria.addGreaterOrEqualThan(StudentAttendance.COL_DATE , StartDate);                                             
    absentCriteria.addLessOrEqualThan(StudentAttendance.COL_DATE , m_edPlan.getEffectiveDate());                              
    QueryByCriteria absentQuery = new QueryByCriteria(StudentAttendance.class, absentCriteria);                               
    absentQuery.addOrderByAscending(StudentAttendance.COL_DATE);                                                              
    QueryIterator absenceCollection = getBroker().getIteratorByQuery(absentQuery);                                            
                                                                                                                              
	                                                                                                                             
	                                                                                                                             
	while (absenceCollection.hasNext())                                                                                          
	{                                                                                                                            
		absenceCollection.next();                                                                                                   
		                                                                                                                            
		 Absences = Absences +1;                                                                                                    
	}                                                                                                                            
	                                                                                                                             
	                                                                                                                             
    return Absences;                                                                                                          
                                                                                                                              
}                                                                                                                             
                                                                                                                              
public void getLongitudeLatitude(String address ) {                                                                           
    try {                                                                                                                     
        StringBuilder urlBuilder = new StringBuilder(GEOCODE_REQUEST_URL);                                                    
        if (StringUtils.isNotBlank(address)) {                                                                                
            urlBuilder.append("&address=").append(URLEncoder.encode(address, "UTF-8"));                                       
        }                                                                                                                     
                                                                                                                              
        final GetMethod getMethod = new GetMethod(urlBuilder.toString());                                                     
        try {                                                                                                                 
            httpClient.executeMethod(getMethod);                                                                              
            Reader reader = new InputStreamReader(getMethod.getResponseBodyAsStream(), getMethod.getResponseCharSet());       
                                                                                                                              
            int data = reader.read();                                                                                         
            char[] buffer = new char[1024];                                                                                   
            Writer writer = new StringWriter();                                                                               
            while ((data = reader.read(buffer)) != -1) {                                                                      
                    writer.write(buffer, 0, data);                                                                            
            }                                                                                                                 
                                                                                                                              
            String result = writer.toString();                                                                                
            System.out.println(result.toString());                                                                            
                                                                                                                              
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();                                                
            DocumentBuilder db = dbf.newDocumentBuilder();                                                                    
            InputSource is = new InputSource();                                                                               
            is.setCharacterStream(new StringReader("<"+writer.toString().trim()));                                            
            Document doc = db.parse(is);                                                                                      
                                                                                                                              
            latstr = getXpathValue(doc, "//GeocodeResponse/result/geometry/location/lat/text()");                             
            lat = (float) Double.parseDouble( latstr)  ;                                                                      
                                                                                                                              
            lonstr = getXpathValue(doc,"//GeocodeResponse/result/geometry/location/lng/text()");                              
            lon = (float) Double.parseDouble(lonstr);                                                                         
                                                                                                                              
            		                                                                                                                
                                                                                                                              
        } finally {                                                                                                           
            getMethod.releaseConnection();                                                                                    
        }                                                                                                                     
    } catch (Exception e) {                                                                                                   
         e.printStackTrace();                                                                                                 
    }                                                                                                                         
}                                                                                                                             
                                                                                                                              
private String getXpathValue(Document doc, String strXpath) throws XPathExpressionException {                                 
    XPath xPath = XPathFactory.newInstance().newXPath();                                                                      
    XPathExpression expr = xPath.compile(strXpath);                                                                           
    String resultData = null;                                                                                                 
    Object result4 = expr.evaluate(doc, XPathConstants.NODESET);                                                              
    NodeList nodes = (NodeList) result4;                                                                                      
    for (int i = 0; i < nodes.getLength(); i++) {                                                                             
        resultData = nodes.item(i).getNodeValue();                                                                            
    }                                                                                                                         
    return resultData;                                                                                                        
}                                                                                                                             
}                                                                                                                             