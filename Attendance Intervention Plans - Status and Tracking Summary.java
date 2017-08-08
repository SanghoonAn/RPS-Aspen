import java.util.Collection;
import java.util.Map;

import org.apache.ojb.broker.query.Criteria;
import org.apache.ojb.broker.query.QueryByCriteria;

import com.follett.fsc.core.k12.beans.DistrictSchoolYearContext;
import com.follett.fsc.core.k12.tools.reports.ReportDataGrid;
import com.follett.fsc.core.k12.tools.reports.ReportJavaSourceNet;
import com.x2dev.sis.model.beans.SisStaff;
import com.x2dev.sis.model.beans.StudentEdPlan; 
import com.x2dev.sis.model.beans.StudentEdPlanMeeting;
import com.x2dev.utils.types.PlainDate;
 
public class INCOMPLETE_EDPLANS extends ReportJavaSourceNet
{
	
/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	
public static final String FIVE_DAY_PLAN = "5 day plan";
public static final String HOME ="Home visit";
public static final String PHONE ="Phone"; 
/**
 *	Start Date Parameter. 
 */	
private static final String							START_DATE						= "StartDate"; 

/**
 *	End Date Parameter.
 */	
private static final String							END_DATE						= "EndDate"; 

public static final String		FIELD_OFFICER_NAME	= "Name";
public static final String		FIELD_PHONECALL		= "Phone";
public static final String		FIELD_HOMEVISIT		= "Home";
public static final String		FIELD_TOTAL			= "Total";
public static final String		FIELD_OLDEST		= "Oldest";

private DistrictSchoolYearContext m_currentYear; 
private Map<String, Collection <StudentEdPlan>> m_edPlans; 
private PlainDate m_endDate;
private PlainDate m_startDate; 

	@SuppressWarnings("deprecation")
	@Override
	
	
	protected Object gatherData() throws Exception {
		 
		m_currentYear =getOrganization().getCurrentContext();
		m_startDate =(PlainDate)getParameter(START_DATE);
		m_endDate =(PlainDate)getParameter(END_DATE);
		
		loadEdPlans();
		ReportDataGrid grid =processEdplans();
		return grid;
	}

	
	public void loadEdPlans()
	{
		Criteria EdCriteria = new Criteria();
		EdCriteria.addEqualTo(StudentEdPlan.COL_STATUS_CODE,  StudentEdPlan.StatusCode.ACTIVE.ordinal() ); 
    	EdCriteria.addEqualTo(StudentEdPlan.COL_FIELD_B075,   FIVE_DAY_PLAN );  
		EdCriteria.addNotNull(StudentEdPlan.COL_STAFF_OID);
		if (m_startDate != null) EdCriteria.addGreaterOrEqualThan(StudentEdPlan.COL_EFFECTIVE_DATE, m_startDate);
		if (m_endDate != null)   EdCriteria.addLessOrEqualThan(StudentEdPlan.COL_EFFECTIVE_DATE, m_endDate);
        QueryByCriteria query = new QueryByCriteria(StudentEdPlan.class, EdCriteria); 
         
        m_edPlans = getBroker().getGroupedCollectionByQuery(query, StudentEdPlan.COL_STAFF_OID, 30);
        
	}
	
	public ReportDataGrid processEdplans()
	{
			ReportDataGrid grid = new ReportDataGrid();  
  
		Collection<String> Officers = m_edPlans.keySet();
		
		for (String officer:Officers)
		{
			SisStaff OfficerBean = (SisStaff) getBroker().getBeanByOid(SisStaff.class, officer);
			String OfficerName =  "";
			if (OfficerBean.getNameView() != null) OfficerName=OfficerBean.getNameView();
			int PhoneCalls =0;
			int Visits = 0;
			int planCount = 0;
			PlainDate oldest = new PlainDate();
			
			Collection<StudentEdPlan> edPlans = m_edPlans.get(officer);
			for (StudentEdPlan edPlan :edPlans)
			{
				boolean phoneFlag = false;
				boolean visitFlag = false; 
				Collection<StudentEdPlanMeeting> Meetings =edPlan.getStudentEdPlanMeetings();
				for (StudentEdPlanMeeting meeting: Meetings)
				{
					if (meeting.getFieldB001() != null)
					{
					if (meeting.getFieldB001().equals(PHONE)) phoneFlag = true;
					if (meeting.getFieldB001().equals(HOME)) visitFlag = true;
					}
				}
				if (edPlan.getEffectiveDate().compareTo(oldest)<0) oldest = edPlan.getEffectiveDate();
				planCount ++;
				if (phoneFlag) PhoneCalls++;
				if (visitFlag) Visits++;
				
			}
			grid.append();
			grid.set(FIELD_OFFICER_NAME, OfficerName);
			grid.set(FIELD_PHONECALL, PhoneCalls);
			grid.set(FIELD_HOMEVISIT, Visits);
			grid.set(FIELD_TOTAL, planCount);
			grid.set(FIELD_OLDEST, oldest);
		}
		grid.beforeTop();
		return grid;
	}
	
	
}