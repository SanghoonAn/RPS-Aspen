import static com.follett.fsc.core.k12.business.ModelProperty.PATH_DELIMITER; 
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException; 
import java.util.logging.Level;

import org.apache.ojb.broker.query.Criteria;
import org.apache.ojb.broker.query.QueryByCriteria;
   
import com.follett.fsc.core.k12.beans.QueryIterator;
import com.follett.fsc.core.k12.tools.reports.ReportDataGrid;
import com.follett.fsc.core.k12.tools.reports.ReportJavaSourceNet;
import com.follett.fsc.core.k12.web.AppGlobals;
import com.follett.fsc.core.k12.web.UserDataContainer; 
import com.x2dev.sis.model.beans.*;
import com.follett.fsc.core.k12.beans.*;
import com.x2dev.utils.types.PlainDate; 

import dori.jasper.engine.JRDataSource;



public class Attendance_intervention_visit_plan_report extends ReportJavaSourceNet {
	
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final String			STUDENT				="student";
	private static final String			DDX				="DDX";
	private static final String			ABSENCES			="absences"; 
    private static final String	DISCARDED                       = "4"; 
    public static final String AIP = "Attendance Intervention Plan";
    /**
     * Name for the enumerated "selection" report parameter. The value is an Integer.
     */
    public static final String QUERY_BY_PARAM = "queryBy";
    
    /**
     * Name for the enumerated "sort" report parameter. The value is an Integer.
     */
    public static final String SORT_PARAM = "sort";
    


	
	private String						activeCode; 
    private String 						wfdOid; 
    private PlainDate 					districtStartDate;
	SisStudent m_currentStudent;
	ReportDataGrid reportGrid = new ReportDataGrid();
	
	@Override
	protected JRDataSource gatherData()
	{
		 
        Criteria criteria = new Criteria(); 
          int queryBy = ((Integer) getParameter(QUERY_BY_PARAM)).intValue();
        switch (queryBy)
        {
            case 0: // Current selection   
                criteria = getCurrentCriteria(); 
               break;
 
            default:            
                // No additional criteria (this is the case for "All")
                break;
        }  
        
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
	 
	 while (activeEdPlans.hasNext())
	 {
	StudentEdPlan	 edPlan = ( StudentEdPlan) activeEdPlans.next();
	reportGrid.append();
    reportGrid.set(STUDENT, student);   
    reportGrid.set(ABSENCES, getAbsences(student,edPlan));  
    reportGrid.beforeTop();
	 } 
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
}