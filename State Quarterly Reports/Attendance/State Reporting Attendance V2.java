package aspen;

import static com.follett.fsc.core.k12.business.ModelProperty.PATH_DELIMITER;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.ojb.broker.query.Criteria;
import org.apache.ojb.broker.query.QueryByCriteria;

import com.follett.fsc.core.framework.persistence.SubQuery;
import com.follett.fsc.core.framework.persistence.X2Criteria;
import com.follett.fsc.core.k12.beans.DistrictSchoolYearContext;
import com.follett.fsc.core.k12.beans.QueryIterator;
import com.follett.fsc.core.k12.beans.SchoolCalendar;
import com.follett.fsc.core.k12.beans.SchoolCalendarDate;
import com.follett.fsc.core.k12.beans.Selection;
import com.follett.fsc.core.k12.beans.SelectionObject;
import com.follett.fsc.core.k12.beans.X2BaseBean;
import com.follett.fsc.core.k12.tools.reports.ReportDataGrid;
import com.follett.fsc.core.k12.tools.reports.ReportJavaSourceNet;
import com.follett.fsc.core.k12.web.UserDataContainer;
import com.x2dev.sis.model.beans.ConductAction;
import com.x2dev.sis.model.beans.ConductActionDate;
import com.x2dev.sis.model.beans.ConductIncident;
import com.x2dev.sis.model.beans.SisSchool;
import com.x2dev.sis.model.beans.SisStudent;
import com.x2dev.sis.model.beans.StudentAttendance;
import com.x2dev.sis.model.beans.StudentEnrollment;
import com.x2dev.utils.types.PlainDate;

public class RPS_STATE_REPORTING extends ReportJavaSourceNet
{

	private static final String							STATUS_ACTIVE = "Active";
	
	private static final String							QUERY_BY = "queryBy";
	 	
	private static final String							DISTRICT_PARAM 					= "districtSummary";

	/**
	 *	Start Date Parameter. 
	 */	
	private static final String							START_DATE						= "StartDate"; 

	/**
	 *	End Date Parameter.
	 */	
	private static final String							END_DATE						= "EndDate"; 
	/**
	 *	Attendance Reason Code for Out of School Suspension.
	 */
	public  static final String 						OS_CODE 						= "OS";

	/**
	 *	Report Field : School
	 */	
	private static final String							FIELD_SCHOOL					= "School";

	/**
	 *	Report Field : All Students ADA number
	 */	
	private static final String							FIELD_ALL_ADA_NUM					= "AllAdaNum";

	/**
	 *	Report Field : All Students ADA Percent
	 */	
	private static final String							FIELD_ALL_ADA_PER					= "ALLADAPER";

	/**
	 *	Report Field : All Students # with 5+ Tardy
	 */	
	private static final String							FIELD_ALL_TAR_NUM					= "AllTarNum";

	/**
	 *	Report Field : All Students % with 5+ Tardy
	 */	
	private static final String							FIELD_ALL_TAR_PER					= "AllTarPer";

	/**
	 *	Report Field : All Students # with 5+ Absences
	 */	
	private static final String							FIELD_ALL_ABS_NUM					= "AllAbsNum";

	/**
	 *	Report Field : All Students % with 5+ Absences
	 */	
	private static final String							FIELD_ALL_ABS_PER					= "AllAbsPer";

	/**
	 *	Report Field : All Students, Total Unexcused Absences
	 */	
	private static final String							FIELD_ALL_UNEX					= "AllUnex";

	/**
	 *	Report Field : All Students, Total Excused Absences
	 */	
	private static final String							FIELD_ALL_EX					= "AllEx";

	/**
	 *	Report Field : All Students Total OS Absences
	 */	
	private static final String							FIELD_ALL_OS					= "AllOs";

	/**
	 *	Report Field : All Students Total IS Absences
	 */	
	private static final String							FIELD_ALL_IS					= "AllIs";

	/**
	 *	Report Field : Disability ADA number
	 */	
	private static final String							FIELD_DIS_ADA_NUM					= "DisAdaNum";

	/**
	 *	Report Field : Disability ADA Percent
	 */	
	private static final String							FIELD_DIS_ADA_PER					= "DisAdaPer";

	/**
	 *	Report Field : Disability # with 5+ Tardy
	 */	
	private static final String							FIELD_DIS_TAR_NUM					= "DisTarNum";

	/**
	 *	Report Field : Disability % with 5+ Tardy
	 */	
	private static final String							FIELD_DIS_TAR_PER					= "DisTarPer";

	/**
	 *	Report Field : Disability # with 5+ Absences
	 */	
	private static final String							FIELD_DIS_ABS_NUM					= "DisAbsNum";

	/**
	 *	Report Field : Disability % with 5+ Absences
	 */	
	private static final String							FIELD_DIS_ABS_PER					= "DisAbsPer";

	/**
	 *	Report Field : Disability, Total Unexcused Absences
	 */	
	private static final String							FIELD_DIS_UNEX					= "DisUnex";

	/**
	 *	Report Field : Disability, Total Excused Absences
	 */	
	private static final String							FIELD_DIS_EX					= "DisEx";

	/**
	 *	Report Field : Disability,  Total OS Absences
	 */		
	private static final String							FIELD_DIS_OS					= "DisOs";

	/**
	 *	Report Field : Disability,  Total IS Absences
	 */		
	private static final String							FIELD_DIS_IS					= "DisIs";

	/**
	 *	Report Field : ELL ADA number
	 */	
	private static final String							FIELD_ELL_ADA_NUM					= "EllAdaNum";

	/**
	 *	Report Field : ELL ADA Percent
	 */	
	private static final String							FIELD_ELL_ADA_PER					= "EllAdaPer";

	/**
	 *	Report Field : ELL # with 5+ Tardy
	 */	
	private static final String							FIELD_ELL_TAR_NUM					= "EllTarNum";

	/**
	 *	Report Field : ELL % with 5+ Tardy
	 */	
	private static final String							FIELD_ELL_TAR_PER					= "EllTarPer";

	/**
	 *	Report Field : ELL # with 5+ Absences
	 */	
	private static final String							FIELD_ELL_ABS_NUM					= "EllAbsNum";

	/**
	 *	Report Field : ELL % with 5+ Absences
	 */	
	private static final String							FIELD_ELL_ABS_PER					= "EllAbsPer";

	/**
	 *	Report Field : ELL, Total Unexcused Absences
	 */	
	private static final String							FIELD_ELL_UNEX					= "EllUnex";

	/**
	 *	Report Field : ELL, Total Excused Absences
	 */	
	private static final String							FIELD_ELL_EX					= "EllEx";

	/**
	 *	Report Field : ELL,  Total OS Absences
	 */	
	private static final String							FIELD_ELL_OS					= "EllOs";

	/**
	 *	Report Field : ELL,  Total IS Absences
	 */	
	private static final String							FIELD_ELL_IS					= "EllIs";

	/**
	 *	Report Field : Homeless ADA number
	 */	
	private static final String							FIELD_HOM_ADA_NUM					= "HomAdaNum";

	/**
	 *	Report Field : Homeless ADA Percent
	 */	
	private static final String							FIELD_HOM_ADA_PER					= "HomAdaPer";

	/**
	 *	Report Field : Homeless # with 5+ Tardy
	 */	
	private static final String							FIELD_HOM_TAR_NUM					= "HomTarNum";

	/**
	 *	Report Field : Homeless % with 5+ Tardy
	 */	
	private static final String							FIELD_HOM_TAR_PER					= "HomTarPer";

	/**
	 *	Report Field : Homeless # with 5+ Absences
	 */	
	private static final String							FIELD_HOM_ABS_NUM					= "HomAbsNum";

	/**
	 *	Report Field : Homeless % with 5+ Absences
	 */	
	private static final String							FIELD_HOM_ABS_PER					= "HomAbsPer";

	/**
	 *	Report Field : Homeless, Total Unexcused Absences
	 */	
	private static final String							FIELD_HOM_UNEX					= "HomUnex";

	/**
	 *	Report Field : Homeless, Total Excused Absences
	 */	
	private static final String							FIELD_HOM_EX					= "HomEx";

	/**
	 *	Report Field : Homeless,  Total OS Absences
	 */	
	private static final String							FIELD_HOM_OS					= "HomOs";

	/**
	 *	Report Field : Homeless,  Total IS Absences
	 */	
	private static final String							FIELD_HOM_IS					= "HomIs";
	

	/**
	 *	Report Field : All, Total Membership
	 */	
	private static final String							FIELD_ALL_MEM					= "AllMem";

	/**
	 *	Report Field : Disability,  Total Membership
	 */	
	private static final String							FIELD_DIS_MEM					= "DisMem";

	/**
	 *	Report Field : Ell,  Total Membership
	 */	
	private static final String							FIELD_ELL_MEM					= "EllMem";

	/**
	 *	Report Field : Homeless,  Total Membership
	 */	
	private static final String							FIELD_HOM_MEM					= "HomMem";

	private static final String[] ISS_CODES	={ "ISS","ISSRG"  };

	private Map <String, Collection<SisStudent>> m_students;
	private Map <String,  SisStudent > m_studentMap;
	private Map<String,HashMap<PlainDate,StudentAttendance> > m_absences; 
	private Selection m_selection; 
	private Map<String, Collection<SchoolCalendarDate>> m_calendarDays; 
	private Map<String,HashMap<String, SchoolCalendar>> m_schoolCalendars  ;
	private PlainDate			m_YearStartDate; 
	private Map<String, Collection<PairedEnrollment >> m_schoolMemberships; 
	private PlainDate m_endDate;
	private PlainDate m_startDate; 
	private Map<String,HashMap<PlainDate,ConductActionDate> > m_actionDates; 
	private SisSchool m_currentSchool;
	private SubQuery m_schoolSub; 
	private Collection<SisSchool> m_schools;
	private boolean m_districtSummary;
	private DistrictSchoolYearContext m_currentYear; 
	
	
	@Override
	protected Object gatherData() throws Exception {
		m_startDate =(PlainDate)getParameter(START_DATE);
		m_endDate =(PlainDate)getParameter(END_DATE);
		
		
		m_schoolSub = getSchoolSubQuery();
		if (m_endDate.compareTo(m_startDate) < 0)
		{
			m_endDate = m_startDate;
		}
		 
		getSchoolYear();
		
		m_districtSummary = false;
		if (!isSchoolContext())
		{
		m_districtSummary = (boolean)getParameter(DISTRICT_PARAM);
		}
		
		
		m_YearStartDate = m_currentYear.getStartDate();
		if (m_endDate.compareTo(m_currentYear.getEndDate())>0 ) m_endDate = m_currentYear.getEndDate();
		
		addParameter(START_DATE,m_startDate );
		addParameter(END_DATE,m_endDate );
		
		ReportDataGrid grid = new ReportDataGrid();  
		LoadStudents();
		loadCalendarMaps();
		LoadAttendance();
		LoadInfractions();
		Map<String,Collection<PairedEnrollment>> Enrollments =populateEnrollments();
		if (m_districtSummary )
		{
			processAttendanceDistrict(grid, Enrollments);
		}
		else
			{
			processAttendance(grid, Enrollments);
			}
		grid.beforeTop();
		return grid;
	}

	public void getSchoolYear()
	{
		Criteria criteria = new Criteria();
		criteria.addGreaterOrEqualThan(DistrictSchoolYearContext.COL_END_DATE, m_startDate);
		criteria.addLessOrEqualThan(DistrictSchoolYearContext.COL_START_DATE, m_startDate);
		QueryByCriteria query =  new QueryByCriteria(DistrictSchoolYearContext.class, criteria);
		m_currentYear = (DistrictSchoolYearContext) getBroker().getBeanByQuery(query); 
	}

	public void LoadStudents()
	{
		Criteria criteria = new Criteria();
		/*if (isSchoolContext())
		{
			criteria.addEqualTo(SisStudent.COL_SCHOOL_OID, getSchool().getOid());
		}  */
		
		
		Criteria criteriaA = new Criteria();
		Criteria criteriaB = new Criteria();
		Criteria criteriaC = new Criteria();
		Criteria criteriaD = new Criteria();
		Criteria comboCriteria = new Criteria();
		Criteria comboCriteria2 = new Criteria();

		// Active student
		criteriaA.addEqualTo(SisStudent.COL_ENROLLMENT_STATUS, STATUS_ACTIVE);
		criteriaA.addIn(SisStudent.COL_SCHOOL_OID, m_schoolSub);

		// OR anyone who has enrollment record this year.
		SubQuery  enrs = getCurrentYearEnrollments() ;
		criteriaB.addIn(SisStudent.COL_OID, enrs );

		comboCriteria.addOrCriteria(criteriaA);
		comboCriteria.addOrCriteria(criteriaB);

		criteria.addAndCriteria(comboCriteria);  //comboCriteria
		
		criteriaC.addEqualTo(  SisStudent.COL_FIELD_A032 , "0");
		criteriaD.addEqualTo(  SisStudent.COL_FIELD_A032 , null);
		comboCriteria2.addOrCriteria(criteriaC);
		comboCriteria2.addOrCriteria(criteriaD);
		
		criteria.addAndCriteria(comboCriteria2);  //comboCriteria

		QueryByCriteria query =  new QueryByCriteria(SisStudent.class, criteria);
		query.setDistinct(true);

		m_students   = getBroker().getGroupedCollectionByQuery(query, SisStudent.COL_SCHOOL_OID, 100); 
		m_studentMap = getBroker().getMapByQuery(query, SisStudent.COL_OID, 1000);

		m_selection  = (Selection) X2BaseBean.newInstance(Selection.class, getBroker().getPersistenceKey());

		// add students to selection object table
		
		Collection<SisStudent>  STU_COL = m_studentMap.values();

		for (SisStudent student : STU_COL)
		{
			SelectionObject selectedObject = (SelectionObject) X2BaseBean.newInstance(SelectionObject.class, getBroker().getPersistenceKey());
			selectedObject.setObjectOid(student.getOid());   
			m_selection.addToSelectionObjects(selectedObject);  
		} 
		m_selection.setTimestamp(System.currentTimeMillis());
		getBroker().saveBean(m_selection);
	}


	public void LoadInfractions()
	{
		List<String> Codes    = new ArrayList<String>(Arrays.asList(ISS_CODES));

		Criteria criteria = new Criteria();
		criteria.addExists(getSubQueryFromSelection(ConductActionDate.COL_STUDENT_OID )); 
		criteria.addIn(ConductActionDate.REL_ACTION + PATH_DELIMITER + ConductAction.COL_ACTION_CODE , Codes);
		QueryByCriteria query =  new QueryByCriteria(ConductActionDate.class, criteria);
		m_actionDates = getBroker().getNestedMapByQuery(query, ConductActionDate.COL_STUDENT_OID, ConductActionDate.COL_DATE , 1000, 200);	
	}

	public void LoadAttendance()
	{
		Criteria criteria = new Criteria();
		criteria.addExists(getSubQueryFromSelection(StudentAttendance.COL_STUDENT_OID )); 
		QueryByCriteria query =  new QueryByCriteria(StudentAttendance.class, criteria);
		query.setDistinct(true);
		m_absences = getBroker().getNestedMapByQuery(query, StudentAttendance.COL_STUDENT_OID, StudentAttendance.COL_DATE, 1000, 200);

	}

	/**
	 * Loads the Calendar Days and School Calendars into two separate Maps.  Maps are used to determine the Schedule day of the run date in
	 * order to determine if the class in question met on the run date.
	 * 
	 * @param none
	 * 
	 * @return none;
	 */
	private void loadCalendarMaps()
	{    
		/* 
		 * Build Calendar Dates Map
		 */
		Criteria criteria = new Criteria(); 
		criteria.addGreaterOrEqualThan(SchoolCalendarDate.COL_DATE,m_startDate );
		criteria.addLessOrEqualThan(SchoolCalendarDate.COL_DATE,m_endDate );
		criteria.addEqualTo(SchoolCalendarDate.COL_IN_SESSION_INDICATOR, true);
		QueryByCriteria query = new QueryByCriteria(SchoolCalendarDate.class, criteria);   
		query.addOrderByAscending(SchoolCalendarDate.COL_SCHOOL_CALENDAR_OID);
		query.addOrderByAscending(SchoolCalendarDate.COL_DATE);
		query.setDistinct(true);
		m_calendarDays = getBroker().getGroupedCollectionByQuery(query, SchoolCalendarDate.COL_SCHOOL_CALENDAR_OID,600);

		/* 
		 * Build School Calendar ID Map
		 */

		criteria = new Criteria();
		criteria.addEqualTo( SchoolCalendar.COL_DISTRICT_CONTEXT_OID, m_currentYear.getOid());  
		query = new QueryByCriteria(SchoolCalendar.class, criteria);
		query.addOrderByDescending(SchoolCalendar.COL_CALENDAR_ID);	 	
		Collection <SchoolCalendar> Cals = getBroker().getCollectionByQuery(query); 
		m_schoolCalendars = getBroker().getNestedMapByQuery(query,SchoolCalendar.COL_SCHOOL_OID,   SchoolCalendar.COL_CALENDAR_ID,200, 5); 
	}


	private SubQuery getCurrentYearEnrollments()
	{
		Criteria criteria = new Criteria();
		criteria.addGreaterOrEqualThan(StudentEnrollment.COL_ENROLLMENT_DATE, m_YearStartDate);
		criteria.addIn(StudentEnrollment.COL_SCHOOL_OID , m_schoolSub);
 
		SubQuery query = new SubQuery(StudentEnrollment.class, StudentEnrollment.COL_STUDENT_OID, criteria);   
		query.setDistinct(true);
		return query;
	}

	private void processAttendance(final ReportDataGrid grid, Map<String,Collection<PairedEnrollment>> Enrollments )
	{

		Collection<String> SchoolOids = m_students.keySet();

		for (String Oid: SchoolOids)
		{
			Collection<SisStudent> students = m_students.get(Oid);

			for (SisStudent student : students)
			{
				if (Enrollments.containsKey(student.getOid()))
				{
				Collection<PairedEnrollment> Enr = Enrollments.get(student.getOid());

				for (PairedEnrollment PE: Enr)
				{
			 		ProcessMembership(PE,  student );
				}
				}
			}
		}

		 

		for(SisSchool school: m_schools)
		{
			String Oid = school.getOid();
			
		if  (m_schoolCalendars.containsKey(Oid) && m_schoolMemberships.containsKey(Oid) && m_students.containsKey(Oid) )
		{
			grid.append();
			String key = m_schoolCalendars.get(Oid).values().iterator().next().getOid();
			int days =   m_calendarDays.get(key).size();
			/// Variables for All Students Row
			BigDecimal 		AAN=BigDecimal.ZERO;
			int 		AANAbs =0;
			int 		AANMem =0;
			BigDecimal 	AAP = BigDecimal.ZERO;
			int 		ATN =0;
			BigDecimal 	ATP = BigDecimal.ZERO;
			int			ABN = 0;
			BigDecimal 	ABP = BigDecimal.ZERO;
			int			AUN = 0;
			int			AEX = 0;
			int			AOS = 0;
			int			AIS = 0;
			/// Variables for Disabled Students Row
			BigDecimal 		DAN=BigDecimal.ZERO;
			int 		DANAbs=0;
			int 		DANMem=0;
			BigDecimal 	DAP = BigDecimal.ZERO;
			int 		DTN =0;
			BigDecimal 	DTP = BigDecimal.ZERO;
			int			DBN = 0;
			BigDecimal 	DBP = BigDecimal.ZERO;
			int			DUN = 0;
			int			DEX = 0;
			int			DOS = 0;
			int			DIS = 0;
			/// Variables for ELL Students Row
			BigDecimal 		EAN=BigDecimal.ZERO;
			int 		EANAbs=0;
			int 		EANMem=0;
			BigDecimal 	EAP = BigDecimal.ZERO;
			int 		ETN =0;
			BigDecimal 	ETP = BigDecimal.ZERO;
			int			EBN = 0;
			BigDecimal 	EBP = BigDecimal.ZERO;
			int			EUN = 0;
			int			EEX = 0;
			int			EOS = 0;
			int			EIS = 0;
			/// Variables for Homeless Students Row
			BigDecimal 		HAN=BigDecimal.ZERO;
			int 		HANAbs=0;
			int 		HANMem=0;
			BigDecimal 	HAP = BigDecimal.ZERO;
			int 		HTN =0;
			BigDecimal 	HTP = BigDecimal.ZERO;
			int			HBN = 0;
			BigDecimal 	HBP = BigDecimal.ZERO;
			int			HUN = 0;
			int			HEX = 0;
			int			HOS = 0;
			int			HIS = 0;
			// Process ADA
			Collection<PairedEnrollment> Enrs = m_schoolMemberships.get(Oid);
			for (PairedEnrollment Enr : Enrs)
			{
				SisStudent student = m_studentMap.get(Enr.studentOid);
				AANAbs= AANAbs + Enr.Absences;
				AANMem=  AANMem +Enr.Membership_days; 
				
				if (student.getSpedStatusCode()  != null &&  student.getSpedStatusCode().equals(STATUS_ACTIVE) )  
				{
					DANAbs= DANAbs+ Enr.Absences;
					DANMem= DANMem + Enr.Membership_days;
				}

				if (student.getFieldB026() != null && !student.getFieldB026().isEmpty() && !student.getFieldB026().equals("3"))
				{
					EANAbs= EANAbs+ Enr.Absences;
					EANMem= EANMem + Enr.Membership_days;
				}

				if (student.getFieldA097() != null && (student.getFieldA097().equals("Y") || student.getFieldA097().equals("1")) )
				{
					HANAbs= HANAbs+ Enr.Absences;
					HANMem= HANMem + Enr.Membership_days;
				}

			}
			// All Students ADA
			BigDecimal NTemp  = new BigDecimal(AANMem).subtract( new BigDecimal( AANAbs));
			AAP = NTemp.divide(new BigDecimal(AANMem),4,3); 

			AAN = NTemp.divide(new BigDecimal(days), 1, RoundingMode.UP); 
			grid.set(FIELD_SCHOOL,  school.getName()); 
			grid.set(FIELD_ALL_ADA_NUM, new BigDecimal (AAN)); // CHange to AAN
			grid.set(FIELD_ALL_ADA_PER, AAP);
			grid.set(FIELD_ALL_MEM, new BigDecimal(AANMem));

			// Disability Students ADA
			if (DANMem >0)
			{
				NTemp  = new BigDecimal(DANMem).subtract( new BigDecimal( DANAbs));
				DAP = NTemp.divide(new BigDecimal(DANMem),4,3); 
				DAN = NTemp.divide(new BigDecimal(days), 1, RoundingMode.UP); 

				grid.set(FIELD_DIS_ADA_NUM, DAN);
				grid.set(FIELD_DIS_ADA_PER, DAP);
				grid.set(FIELD_DIS_MEM, new BigDecimal(DANMem));
			}
			else
			{ 
				grid.set(FIELD_DIS_ADA_NUM, null);
				grid.set(FIELD_DIS_ADA_PER, null);
				grid.set(FIELD_DIS_MEM, new BigDecimal(DANMem));

			}


			// ELL Students ADA
			if (EANMem >0)
			{
				NTemp  = new BigDecimal(EANMem).subtract( new BigDecimal( EANAbs));
				EAP = NTemp.divide(new BigDecimal(EANMem),4,3); 
				EAN = NTemp.divide(new BigDecimal(days), 1, RoundingMode.UP);	 
				grid.set(FIELD_ELL_ADA_NUM, EAN);
				grid.set(FIELD_ELL_ADA_PER, EAP);
				grid.set(FIELD_ELL_MEM, new BigDecimal(EANMem));
			}
			else
			{
				grid.set(FIELD_ELL_ADA_NUM, null);
				grid.set(FIELD_ELL_ADA_PER, null);
				grid.set(FIELD_ELL_MEM, new BigDecimal(EANMem));
			}

			// Homeless Students ADA
			if (HANMem >0)
			{
				NTemp  = new BigDecimal(HANMem).subtract( new BigDecimal( HANAbs));
				HAP = NTemp.divide(new BigDecimal(HANMem),4,3); 
				HAN = NTemp.divide(new BigDecimal(days), 1, RoundingMode.UP); 
				grid.set(FIELD_HOM_ADA_NUM, HAN);
				grid.set(FIELD_HOM_ADA_PER, HAP);
				grid.set(FIELD_HOM_MEM, new BigDecimal(HANMem));
			}
			else
			{
				grid.set(FIELD_HOM_ADA_NUM, null);
				grid.set(FIELD_HOM_ADA_PER, null);	
				grid.set(FIELD_HOM_MEM, new BigDecimal(HANMem));
			}

			/// Process non-ADA stats
			Collection<SisStudent> students = m_students.get(Oid);
			int studentCount = 0;
			int disCount = 0;
			int EllCount = 0;
			int HomeCount = 0;

			for (SisStudent student : students)
			{
			if (student.getEnrollmentStatus() != null &&student.getEnrollmentStatus().equals(STATUS_ACTIVE) && Enrollments != null && Enrollments.containsKey(student.getOid()))
			{
				Collection<PairedEnrollment> Enr = Enrollments.get(student.getOid());
				int AllAbsNum = 0;
				int AllTarNum =0;
				int DisAbsNum = 0;
				int DisTarNum = 0;
				int EllAbsNum =0;
				int EllTarNum = 0;
				int HomAbsNum = 0;
				int HomTarNum = 0;

				if (Enr !=null )
				{
					studentCount = studentCount +1;	
					for (PairedEnrollment PE: Enr)
					{
						AllAbsNum = AllAbsNum+ PE.Absences;
						AllTarNum = AllTarNum + PE.Tardy;
						AUN = AUN+ PE.Unexcused;
						AEX = AEX+ PE.Excused;
						AOS = AOS + PE.OSS;
						AIS = AIS + PE.ISS;

						 if (student.getSpedStatusCode()  != null &&  student.getSpedStatusCode().equals(STATUS_ACTIVE) )  
						{
							disCount = disCount +1;
							DisAbsNum= DisAbsNum+ PE.Absences;
							DisTarNum= DisTarNum + PE.Tardy;
							DUN = DUN+ PE.Unexcused;
							DEX = DEX+ PE.Excused;
							DOS = DOS + PE.OSS;
							DIS = DIS + PE.ISS;
						}

						if (student.getFieldB026() != null && !student.getFieldB026().isEmpty() && !student.getFieldB026().equals("3"))
						{
							EllCount = EllCount+1;
							EllAbsNum= EllAbsNum+ PE.Absences;
							EllTarNum= EllTarNum + PE.Tardy;
							EUN = EUN+ PE.Unexcused;
							EEX = EEX+ PE.Excused;
							EOS = EOS + PE.OSS;
							EIS = EIS + PE.ISS;
						}

						if (student.getFieldA097() != null && (student.getFieldA097().equals("Y") || student.getFieldA097().equals("1")) )
						{
							HomeCount = HomeCount+1;
							HomAbsNum= HomAbsNum+ PE.Absences;
							HomTarNum= HomTarNum + PE.Tardy;
							HUN = HUN+ PE.Unexcused;
							HEX = HEX+ PE.Excused;
							HOS = HOS + PE.OSS;
							HIS = HIS + PE.ISS;
						} 
					}

					if (AllAbsNum >=5) {ABN = ABN+1; }
					if (AllTarNum >=5) {ATN = ATN+1; }
					if (DisAbsNum >=5) {DBN = DBN+1; }
					if (DisTarNum >=5) {DTN = DTN+1; } 
					if (EllAbsNum >=5) {EBN = EBN+1; }
					if (EllTarNum >=5) {ETN = ETN+1; } 
					if (HomAbsNum >=5) {HBN = HBN+1; }
					if (HomTarNum >=5) {HTN = HTN+1; } 
				}
			}

			if (studentCount >0)
			{
			ABP = new BigDecimal(ABN ).divide(new BigDecimal(studentCount),4,3);
			ATP = new BigDecimal(ATN ).divide(new BigDecimal(studentCount),4,3);
			}

			grid.set(FIELD_ALL_ABS_NUM, ABN);
			grid.set(FIELD_ALL_ABS_PER, ABP);
			grid.set(FIELD_ALL_TAR_NUM, ATN);
			grid.set(FIELD_ALL_TAR_PER, ATP);
			grid.set(FIELD_ALL_UNEX, AUN);
			grid.set(FIELD_ALL_EX, AEX);
			grid.set(FIELD_ALL_OS, AOS);
			grid.set(FIELD_ALL_IS, AIS);

			grid.set(FIELD_DIS_UNEX, 	DUN);
			grid.set(FIELD_DIS_EX, 		DEX);
			grid.set(FIELD_DIS_OS, 		DOS);
			grid.set(FIELD_DIS_IS, 		DIS);
			grid.set(FIELD_ELL_UNEX, 	EUN);
			grid.set(FIELD_ELL_EX, 		EEX);
			grid.set(FIELD_ELL_OS, 		EOS);
			grid.set(FIELD_ELL_IS, 		EIS);
			grid.set(FIELD_HOM_UNEX, 	HUN);
			grid.set(FIELD_HOM_EX, 		HEX);
			grid.set(FIELD_HOM_OS, 		HOS);
			grid.set(FIELD_HOM_IS, 		HIS);

			if (disCount >0){
				DBP = new BigDecimal(DBN ).divide(new BigDecimal(disCount),4,3);
				DTP = new BigDecimal(DTN ).divide(new BigDecimal(disCount),4,3);    
				grid.set(FIELD_DIS_ABS_NUM, DBN);
				grid.set(FIELD_DIS_ABS_PER, DBP);
				grid.set(FIELD_DIS_TAR_NUM, DTN);
				grid.set(FIELD_DIS_TAR_PER, DTP);

			}
			else
			{
				grid.set(FIELD_DIS_ABS_NUM, null);
				grid.set(FIELD_DIS_ABS_PER, null);
				grid.set(FIELD_DIS_TAR_NUM, null);
				grid.set(FIELD_DIS_TAR_PER, null);

			}

			if (EllCount >0){
				EBP = new BigDecimal(EBN ).divide(new BigDecimal(EllCount),4,3);
				ETP = new BigDecimal(ETN ).divide(new BigDecimal(EllCount),4,3);    
				grid.set(FIELD_ELL_ABS_NUM, EBN);
				grid.set(FIELD_ELL_ABS_PER, EBP);
				grid.set(FIELD_ELL_TAR_NUM, ETN);
				grid.set(FIELD_ELL_TAR_PER, ETP);
			}
			else
			{
				grid.set(FIELD_ELL_ABS_NUM, null);
				grid.set(FIELD_ELL_ABS_PER, null);
				grid.set(FIELD_ELL_TAR_NUM, null);
				grid.set(FIELD_ELL_TAR_PER, null);

			}

			if (HomeCount >0){
				HBP = new BigDecimal(HBN ).divide(new BigDecimal(HomeCount),4,3);
				HTP = new BigDecimal(HTN ).divide(new BigDecimal(HomeCount),4,3);    
				grid.set(FIELD_HOM_ABS_NUM, HBN);
				grid.set(FIELD_HOM_ABS_PER, HBP);
				grid.set(FIELD_HOM_TAR_NUM, HTN);
				grid.set(FIELD_HOM_TAR_PER, HTP);
			}
			else
			{
				grid.set(FIELD_HOM_ABS_NUM, null);
				grid.set(FIELD_HOM_ABS_PER, null);
				grid.set(FIELD_HOM_TAR_NUM, null);
				grid.set(FIELD_HOM_TAR_PER, null);

			}
		 }
		}
		} // for each school

	}


	
	private void processAttendanceDistrict(final ReportDataGrid grid, Map<String,Collection<PairedEnrollment>> Enrollments )
	{

		Collection<String> SchoolOids = m_students.keySet();

		for (String Oid: SchoolOids)
		{
			Collection<SisStudent> students = m_students.get(Oid);

			for (SisStudent student : students)
			{
				if (Enrollments.containsKey(student.getOid()))
				{
				Collection<PairedEnrollment> Enr = Enrollments.get(student.getOid());

				for (PairedEnrollment PE: Enr)
				{
			 		ProcessMembership(PE,  student );
				}
				}
			}
		}

		 grid.append();
		 	BigDecimal	AAN= BigDecimal.ZERO;
			int 		AANAbsC =0;
			int 		AANMemC =0;
			BigDecimal 	AAP = BigDecimal.ZERO;
			int 		ATN =0;
			BigDecimal 	ATP = BigDecimal.ZERO;
			int			ABN = 0;
			BigDecimal 	ABP = BigDecimal.ZERO;
			int			AUN = 0;
			int			AEX = 0;
			int			AOS = 0;
			int			AIS = 0;
			/// Variables for Disabled Students Row
			BigDecimal	DAN=BigDecimal.ZERO;
			int 		DANAbsC=0;
			int 		DANMemC=0;
			BigDecimal 	DAP = BigDecimal.ZERO;
			int 		DTN =0;
			BigDecimal 	DTP = BigDecimal.ZERO;
			int			DBN = 0;
			BigDecimal 	DBP = BigDecimal.ZERO;
			int			DUN = 0;
			int			DEX = 0;
			int			DOS = 0;
			int			DIS = 0;
			/// Variables for ELL Students Row
			BigDecimal EAN=BigDecimal.ZERO;
			int 		EANAbsC=0;
			int 		EANMemC=0;
			BigDecimal 	EAP = BigDecimal.ZERO;
			int 		ETN =0;
			BigDecimal 	ETP = BigDecimal.ZERO;
			int			EBN = 0;
			BigDecimal 	EBP = BigDecimal.ZERO;
			int			EUN = 0;
			int			EEX = 0;
			int			EOS = 0;
			int			EIS = 0;
			/// Variables for Homeless Students Row
			BigDecimal  HAN=BigDecimal.ZERO;
			int 		HANAbsC=0;
			int 		HANMemC=0;
			BigDecimal 	HAP = BigDecimal.ZERO;
			int 		HTN =0;
			BigDecimal 	HTP = BigDecimal.ZERO;
			int			HBN = 0;
			BigDecimal 	HBP = BigDecimal.ZERO;
			int			HUN = 0;
			int			HEX = 0;
			int			HOS = 0;
			int			HIS = 0;
			
			int studentCount = 0;
			int disCount = 0;
			int EllCount = 0;
			int HomeCount = 0;
			
		for(SisSchool school: m_schools)
		{
			String Oid = school.getOid();
			int AANAbs = 0;
			int AANMem = 0;
			int DANAbs = 0;
			int DANMem = 0;
			int EANAbs = 0;
			int EANMem = 0;
			int HANAbs = 0;
			int HANMem = 0;
		if  (m_schoolCalendars.containsKey(Oid) && m_schoolMemberships.containsKey(Oid) )
		{
			
			String key = m_schoolCalendars.get(Oid).values().iterator().next().getOid();
			int days =   m_calendarDays.get(key).size();
			/// Variables for All Students Row
			
			// Process ADA
			Collection<PairedEnrollment> Enrs = m_schoolMemberships.get(Oid);
			for (PairedEnrollment Enr : Enrs)
			{
				SisStudent student = m_studentMap.get(Enr.studentOid);
				AANAbs= AANAbs + Enr.Absences;
				AANMem=  AANMem +Enr.Membership_days; 
				
				if (student.getSpedStatusCode()  != null &&  student.getSpedStatusCode().equals(STATUS_ACTIVE) )  
				{
					DANAbs= DANAbs+ Enr.Absences;
					DANMem= DANMem + Enr.Membership_days;
				}

				if (student.getFieldB026() != null && !student.getFieldB026().isEmpty() && !student.getFieldB026().equals("3"))
				{
					EANAbs= EANAbs+ Enr.Absences;
					EANMem= EANMem + Enr.Membership_days;
				}

				if (student.getFieldA097() != null && (student.getFieldA097().equals("Y") || student.getFieldA097().equals("1")) )
				{
					HANAbs= HANAbs+ Enr.Absences;
					HANMem= HANMem + Enr.Membership_days;
				}

			}
			AANAbsC= AANAbsC+AANAbs;
			AANMemC= AANMemC+AANMem;
			DANAbsC= DANAbsC+DANAbs;
			DANMemC= DANMemC+DANMem;
			EANAbsC= EANAbsC+EANAbs;
			EANMemC= EANMemC+EANMem;
			HANAbsC= HANAbsC+HANAbs;
			HANMemC= HANMemC+HANMem;
			
			
			BigDecimal NTemp  = new BigDecimal(AANMem).subtract( new BigDecimal( AANAbs));
			//AAP = NTemp.divide(new BigDecimal(AANMem),4,1);
			//AAP = AAP.multiply(new BigDecimal(100));

			AAN  =  AAN.add( NTemp.divide(new BigDecimal(days), 2, RoundingMode.HALF_UP));
		
			// Disability Students ADA
			if (DANMem >0)
			{
				NTemp  = new BigDecimal(DANMem).subtract( new BigDecimal( DANAbs));
				DAP = NTemp.divide(new BigDecimal(DANMem),4,3); 
				DAN = DAN.add(NTemp.divide(new BigDecimal(days), 1, RoundingMode.UP));  
			} 
			// ELL Students ADA
			if (EANMem >0)
			{
				NTemp  = new BigDecimal(EANMem).subtract( new BigDecimal( EANAbs));
				EAP = NTemp.divide(new BigDecimal(EANMem),4,3); 
				EAN = EAN.add(NTemp.divide(new BigDecimal(days), 1, RoundingMode.UP)); 
			} 

			// Homeless Students ADA
			if (HANMem >0)
			{
				NTemp  = new BigDecimal(HANMem).subtract( new BigDecimal( HANAbs));
				HAP = NTemp.divide(new BigDecimal(HANMem),4,3); 
				HAN = HAN.add(NTemp.divide(new BigDecimal(days), 1, RoundingMode.UP));
			}

			/// Process non-ADA stats
			if ( m_students.containsKey(Oid))
			{
			Collection<SisStudent> students = m_students.get(Oid);
	 
			for (SisStudent student : students)
			{
			if (student.getEnrollmentStatus().equals(STATUS_ACTIVE) && Enrollments.containsKey(student.getOid()))
			{
				Collection<PairedEnrollment> Enr = Enrollments.get(student.getOid());
				int AllAbsNum = 0;
				int AllTarNum =0;
				int DisAbsNum = 0;
				int DisTarNum = 0;
				int EllAbsNum =0;
				int EllTarNum = 0;
				int HomAbsNum = 0;
				int HomTarNum = 0;

				if (Enr !=null )
				{
					studentCount = studentCount +1;	
					for (PairedEnrollment PE: Enr)
					{
						AllAbsNum = AllAbsNum+ PE.Absences;
						AllTarNum = AllTarNum + PE.Tardy;
						AUN = AUN+ PE.Unexcused;
						AEX = AEX+ PE.Excused;
						AOS = AOS + PE.OSS;
						AIS = AIS + PE.ISS;

						if (student.getSpedStatusCode()  != null &&  student.getSpedStatusCode().equals(STATUS_ACTIVE) )  
						{
							disCount = disCount +1;
							DisAbsNum= DisAbsNum+ PE.Absences;
							DisTarNum= DisTarNum + PE.Tardy;
							DUN = DUN+ PE.Unexcused;
							DEX = DEX+ PE.Excused;
							DOS = DOS + PE.OSS;
							DIS = DIS + PE.ISS;
						}

						if (student.getFieldB026() != null && !student.getFieldB026().isEmpty() && !student.getFieldB026().equals("3"))
						{
							EllCount = EllCount+1;
							EllAbsNum= EllAbsNum+ PE.Absences;
							EllTarNum= EllTarNum + PE.Tardy;
							EUN = EUN+ PE.Unexcused;
							EEX = EEX+ PE.Excused;
							EOS = EOS + PE.OSS;
							EIS = EIS + PE.ISS;
						}

						if (student.getFieldA097() != null && (student.getFieldA097().equals("Y") || student.getFieldA097().equals("1")) )
						{
							HomeCount = HomeCount+1;
							HomAbsNum= HomAbsNum+ PE.Absences;
							HomTarNum= HomTarNum + PE.Tardy;
							HUN = HUN+ PE.Unexcused;
							HEX = HEX+ PE.Excused;
							HOS = HOS + PE.OSS;
							HIS = HIS + PE.ISS;
						} 
					}

					if (AllAbsNum >=5) {ABN = ABN+1; }
					if (AllTarNum >=5) {ATN = ATN+1; }
					if (DisAbsNum >=5) {DBN = DBN+1; }
					if (DisTarNum >=5) {DTN = DTN+1; } 
					if (EllAbsNum >=5) {EBN = EBN+1; }
					if (EllTarNum >=5) {ETN = ETN+1; } 
					if (HomAbsNum >=5) {HBN = HBN+1; }
					if (HomTarNum >=5) {HTN = HTN+1; } 
				}
			}
 
		 }
			}
		}
		} // for each school
		// All Students ADA
					BigDecimal NTemp  = new BigDecimal(AANMemC).subtract( new BigDecimal( AANAbsC));
					AAP = NTemp.divide(new BigDecimal(AANMemC),4,3); 

 
					grid.set(FIELD_SCHOOL, "District Summary"); 
					grid.set(FIELD_ALL_ADA_NUM, AAN);
					grid.set(FIELD_ALL_ADA_PER, AAP);
					grid.set(FIELD_ALL_MEM, new BigDecimal(AANMemC));

					// Disability Students ADA
					if (DANMemC >0)
					{
						NTemp  = new BigDecimal(DANMemC).subtract( new BigDecimal( DANAbsC));
						DAP = NTemp.divide(new BigDecimal(DANMemC),4,3); 
 

						grid.set(FIELD_DIS_ADA_NUM, DAN);
						grid.set(FIELD_DIS_ADA_PER, DAP);
						grid.set(FIELD_DIS_MEM,new BigDecimal( AANMemC));
					}
					else
					{ 
						grid.set(FIELD_DIS_ADA_NUM, null);
						grid.set(FIELD_DIS_ADA_PER, null);
						grid.set(FIELD_DIS_MEM, new BigDecimal(AANMemC));

					}


					// ELL Students ADA
					if (EANMemC >0)
					{
						NTemp  = new BigDecimal(EANMemC).subtract( new BigDecimal( EANAbsC));
						EAP = NTemp.divide(new BigDecimal(EANMemC),4,3); 
 
						grid.set(FIELD_ELL_ADA_NUM, EAN);
						grid.set(FIELD_ELL_ADA_PER, EAP);
						grid.set(FIELD_ELL_MEM, new BigDecimal(EANMemC));
					}
					else
					{
						grid.set(FIELD_ELL_ADA_NUM, null);
						grid.set(FIELD_ELL_ADA_PER, null);
						grid.set(FIELD_ELL_MEM, new BigDecimal(EANMemC));
					}

					// Homeless Students ADA
					if (HANMemC >0)
					{
						NTemp  = new BigDecimal(HANMemC).subtract( new BigDecimal( HANAbsC));
						HAP = NTemp.divide(new BigDecimal(HANMemC),4,3); 
 
						grid.set(FIELD_HOM_ADA_NUM, HAN);
						grid.set(FIELD_HOM_ADA_PER, HAP);
						grid.set(FIELD_HOM_MEM, new BigDecimal(HANMemC));
					}
					else
					{
						grid.set(FIELD_HOM_ADA_NUM, null);
						grid.set(FIELD_HOM_ADA_PER, null);	
						grid.set(FIELD_HOM_MEM, new BigDecimal(HANMemC));
					}
					
					if (studentCount >0)
					{
					ABP = new BigDecimal(ABN ).divide(new BigDecimal(studentCount),4,3);
					ATP = new BigDecimal(ATN ).divide(new BigDecimal(studentCount),4,3);
					}

					grid.set(FIELD_ALL_ABS_NUM, ABN);
					grid.set(FIELD_ALL_ABS_PER, ABP);
					grid.set(FIELD_ALL_TAR_NUM, ATN);
					grid.set(FIELD_ALL_TAR_PER, ATP);
					grid.set(FIELD_ALL_UNEX, AUN);
					grid.set(FIELD_ALL_EX, AEX);
					grid.set(FIELD_ALL_OS, AOS);
					grid.set(FIELD_ALL_IS, AIS);

					grid.set(FIELD_DIS_UNEX, 	DUN);
					grid.set(FIELD_DIS_EX, 		DEX);
					grid.set(FIELD_DIS_OS, 		DOS);
					grid.set(FIELD_DIS_IS, 		DIS);
					grid.set(FIELD_ELL_UNEX, 	EUN);
					grid.set(FIELD_ELL_EX, 		EEX);
					grid.set(FIELD_ELL_OS, 		EOS);
					grid.set(FIELD_ELL_IS, 		EIS);
					grid.set(FIELD_HOM_UNEX, 	HUN);
					grid.set(FIELD_HOM_EX, 		HEX);
					grid.set(FIELD_HOM_OS, 		HOS);
					grid.set(FIELD_HOM_IS, 		HIS);

					if (disCount >0){
						DBP = new BigDecimal(DBN).divide(new BigDecimal(disCount),4,3);
						DTP = new BigDecimal(DTN ).divide(new BigDecimal(disCount),4,3);    
						grid.set(FIELD_DIS_ABS_NUM, DBN);
						grid.set(FIELD_DIS_ABS_PER, DBP);
						grid.set(FIELD_DIS_TAR_NUM, DTN);
						grid.set(FIELD_DIS_TAR_PER, DTP);

					}
					else
					{
						grid.set(FIELD_DIS_ABS_NUM, null);
						grid.set(FIELD_DIS_ABS_PER, null);
						grid.set(FIELD_DIS_TAR_NUM, null);
						grid.set(FIELD_DIS_TAR_PER, null);

					}

					if (EllCount >0){
						EBP = new BigDecimal(EBN ).divide(new BigDecimal(EllCount),4,3);
						ETP = new BigDecimal(ETN ).divide(new BigDecimal(EllCount),4,3);    
						grid.set(FIELD_ELL_ABS_NUM, EBN);
						grid.set(FIELD_ELL_ABS_PER, EBP);
						grid.set(FIELD_ELL_TAR_NUM, ETN);
						grid.set(FIELD_ELL_TAR_PER, ETP);
					}
					else
					{
						grid.set(FIELD_ELL_ABS_NUM, null);
						grid.set(FIELD_ELL_ABS_PER, null);
						grid.set(FIELD_ELL_TAR_NUM, null);
						grid.set(FIELD_ELL_TAR_PER, null);

					}

					if (HomeCount >0){
						HBP = new BigDecimal(HBN ).divide(new BigDecimal(HomeCount),4,3);
						HTP = new BigDecimal(HTN ).divide(new BigDecimal(HomeCount),4,3);    
						grid.set(FIELD_HOM_ABS_NUM, HBN);
						grid.set(FIELD_HOM_ABS_PER, HBP);
						grid.set(FIELD_HOM_TAR_NUM, HTN);
						grid.set(FIELD_HOM_TAR_PER, HTP);
					}
					else
					{
						grid.set(FIELD_HOM_ABS_NUM, null);
						grid.set(FIELD_HOM_ABS_PER, null);
						grid.set(FIELD_HOM_TAR_NUM, null);
						grid.set(FIELD_HOM_TAR_PER, null);

					}


	}

	
	
	/**
	 * Returns a sub query for the list of students to include in the query.
	 * 
	 * @param beanPath
	 * 
	 * @return SubQuery;
	 */
	private SubQuery getSubQueryFromSelection(String beanPath)
	{ 

		SubQuery studentSubQuery = null;

		Criteria subCriteria = new Criteria();
		subCriteria.addEqualTo(SelectionObject.COL_SELECTION_OID, m_selection.getOid());
		subCriteria.addEqualToField(SelectionObject.COL_OBJECT_OID, Criteria.PARENT_QUERY_PREFIX
				+ beanPath);

		studentSubQuery = new SubQuery(SelectionObject.class, X2BaseBean.COL_OID, subCriteria); 
		return studentSubQuery;

	}

	
	/**
	 * Returns a sub query for the list of Schools to include in the query.
	 * 
	 * @param beanPath
	 * 
	 * @return SubQuery;
	 */
	private SubQuery getSchoolSubQuery( )
	{ 
 
		Criteria subCriteria = new Criteria(); 
		
		if (isSchoolContext()){
			subCriteria.addEqualTo(SisSchool.COL_OID, getSchool().getOid());
		}
		else if (m_currentSchool != null )
		{
			subCriteria.addEqualTo(SisSchool.COL_OID, m_currentSchool.getOid());
		} 
		else 
		{
		String queryString=  (String) getParameter(QUERY_BY);  
		
		if (queryString!= null && queryString.equals("##current") && !m_districtSummary)
		{
			Criteria criteria = new Criteria();
			criteria.addEqualTo(SisSchool.COL_INACTIVE_INDICATOR, false);
			
			subCriteria.addAndCriteria(getCurrentCriteria());
			subCriteria.addAndCriteria(criteria);		 
		}
		else
		{
			subCriteria.addEqualTo(SisSchool.COL_INACTIVE_INDICATOR, false);
		}
		}

		SubQuery schoolSubQuery = new SubQuery(SisSchool.class, SisSchool.COL_OID, subCriteria);
		QueryByCriteria  schoolQuery = new QueryByCriteria(SisSchool.class,   subCriteria); 
		m_schools = getBroker().getCollectionByQuery(schoolQuery);
		
		return schoolSubQuery;

	}
	
	/**
	 * @see com.x2dev.sis.tools.ToolJavaSource#saveState(com.x2dev.sis.web.UserDataContainer)
	 */
	@Override
	protected void saveState(final UserDataContainer userData)
	{
		/*
		 * If we're in the context of a single student, print the report for just that student
		 */
		m_currentSchool = (SisSchool) userData.getCurrentRecord(SisSchool.class);
	}

	private void ProcessMembership(final PairedEnrollment enrollment, SisStudent student )
	{
		if (m_schoolCalendars.containsKey(enrollment.SchoolOid))
		{
		String mkey = m_schoolCalendars.get(enrollment.SchoolOid).values().iterator().next().getOid();
		
		Collection<SchoolCalendarDate> schoolDays =   m_calendarDays.get(mkey);

		HashMap<PlainDate, StudentAttendance> absences = null;
		if (m_absences.containsKey(student.getOid()))
		{
			absences =m_absences.get(student.getOid());
		}
		
		HashMap<PlainDate, ConductActionDate> actions = null;

		if (m_actionDates.containsKey(student.getOid()))
		{
			actions = m_actionDates.get(student.getOid());
		}
		
		
		for (SchoolCalendarDate SDay: schoolDays)
		{
			PlainDate date = SDay.getDate();
			if (date.compareTo(m_startDate) >=0 && date.compareTo(m_endDate ) <=0
					&& ( enrollment.RegistrationDate  != null && date.compareTo(enrollment.RegistrationDate ) >=0)  
					&& (enrollment.WithdrawDate == null || (enrollment.WithdrawDate != null && date.compareTo(enrollment.WithdrawDate ) <=0)))
			{
				StudentAttendance Abs = null;
				enrollment.Membership_days = enrollment.Membership_days +1;
				if (absences != null && absences.containsKey(date))
				{
					Abs = absences.get(date);
				}

				if (Abs != null)
				{
					if(Abs.getAbsentIndicator()) // if Absent
					{
						enrollment.Absences =enrollment.Absences  +1;
						if ( Abs.getExcusedIndicator() ) // If Excused
						{
							enrollment.Excused =enrollment.Excused  +1; 
						}
						else  //If not then increment Unexcused
						{
							enrollment.Unexcused =enrollment.Unexcused  +1; 
						}

						if ((Abs.getReasonCode()!= null && Abs.getReasonCode().equals(OS_CODE)) || (Abs.getCodeView()!= null &&Abs.getCodeView().equals("A-E OS")))
						{
							enrollment.OSS = enrollment.OSS+1;
						}
					}
					else
					{
						if (Abs.getTardyIndicator())
						{
							enrollment.Tardy = enrollment.Tardy  +1;
						}
					} 
				}

				// ISS

				if (actions != null && actions.containsKey(date))
				{
					enrollment.ISS = enrollment.ISS+1;

				}  
			}
		} 
		}
	}
 
 


	private HashMap<String,Collection<PairedEnrollment>> populateEnrollments()
	{
		HashMap<String,Collection<PairedEnrollment>> results = new HashMap<String,Collection<PairedEnrollment>>();
		m_schoolMemberships = new HashMap<String, Collection<PairedEnrollment >>();
		X2Criteria m_enrollmentCriteria = new X2Criteria();  
		m_enrollmentCriteria.addNotEqualTo(StudentEnrollment.COL_ENROLLMENT_TYPE, "Y");
		m_enrollmentCriteria.addExists(getSubQueryFromSelection(StudentEnrollment.COL_STUDENT_OID ));
		QueryByCriteria query = new QueryByCriteria(StudentEnrollment.class, m_enrollmentCriteria);
		query.addOrderByAscending( StudentEnrollment.COL_STUDENT_OID  );
		query.addOrderByAscending(StudentEnrollment.COL_ENROLLMENT_DATE );
		query.addOrderByAscending(StudentEnrollment.COL_TIMESTAMP );
		query.setDistinct(true);

		QueryIterator enrollmentRecordsIterator = getBroker().getIteratorByQuery(query);
		String LastStdOid = null;
		Collection<PairedEnrollment> collection = new HashSet();
		PairedEnrollment Pair = null;
		while (enrollmentRecordsIterator.hasNext())
		{ 
		
			StudentEnrollment enrollmentRecord = (StudentEnrollment) enrollmentRecordsIterator.next();
			if (enrollmentRecord.getSchoolOid() !=null && enrollmentRecord.getStudentOid()!=null && enrollmentRecord.getEnrollmentDate() != null )
				{
			String studentOid = enrollmentRecord.getStudentOid();

			if (LastStdOid== null)  /// initialize loop
			{
				LastStdOid = studentOid;
			}

			if (!LastStdOid.equals(studentOid))  /// if new student then wrap up last student.
			{
				if (Pair != null )
				{
					collection.add(Pair);
					if (!m_schoolMemberships.containsKey(Pair.SchoolOid))
					{
						Collection<PairedEnrollment> temp = new HashSet();
						m_schoolMemberships.put(Pair.SchoolOid,  temp);
					}
					m_schoolMemberships.get(Pair.SchoolOid).add(Pair);

					Pair = null;
				}

				if (collection.size() >0)
				{
					results.put(LastStdOid, collection);
					collection = new HashSet();
				} 
			}


			/// if a Enrollment Record then Create a new Paired Enrollment Record.
			if (enrollmentRecord.getEnrollmentType() != null && enrollmentRecord.getEnrollmentType().equals("E") && enrollmentRecord.getEnrollmentDate() != null)
			{	 
				Pair = new PairedEnrollment(enrollmentRecord.getSchoolOid(), enrollmentRecord.getStudentOid(), enrollmentRecord.getEnrollmentDate());  
			}

			/// If the Withdraw Record
			if (enrollmentRecord.getEnrollmentType() != null && enrollmentRecord.getEnrollmentType().equals("W") && Pair != null )
			{
				//	logMessage ( "Withdraw Record "+ enrollmentRecord.getOid());
				if (Pair.SchoolOid.equals( enrollmentRecord.getSchoolOid()) ) // && enrollmentRecord.getEnrollmentDate().compareTo(m_YearStartDate) >0)
				{
					Pair.WithdrawDate = enrollmentRecord.getEnrollmentDate(); 
					collection.add(Pair); 
					if (!m_schoolMemberships.containsKey(Pair.SchoolOid))
					{
						Collection<PairedEnrollment> temp = new HashSet();
						m_schoolMemberships.put(Pair.SchoolOid,  temp);
					}
					m_schoolMemberships.get(Pair.SchoolOid).add(Pair);
					Pair = null;					
				}				
			}	
			LastStdOid = studentOid;
		} 
		}
		if (Pair != null )
		{
			collection.add(Pair); 
			if (!m_schoolMemberships.containsKey(Pair.SchoolOid))
			{
				Collection<PairedEnrollment> temp = new HashSet();
				m_schoolMemberships.put(Pair.SchoolOid,  temp);
			}
			m_schoolMemberships.get(Pair.SchoolOid).add(Pair);
			//	logMessage ("Pair " + Pair.SchoolOid + " Reg date " + Pair.RegistrationDate +" Added");
		}

		if (collection.size() >0)
		{
			results.put(LastStdOid, collection); 
		}
		
		enrollmentRecordsIterator.close(); 

		return results;
	}

	public class PairedEnrollment
	{
		public PlainDate RegistrationDate;
		public String studentOid;
		public PlainDate WithdrawDate;
		public String SchoolOid;
		public Integer Membership_days;
		public Integer Absences;
		public Integer Tardy;
		public Integer Unexcused;
		public Integer Excused;
		public Integer OSS;
		public Integer ISS;


		public PairedEnrollment( String SOid, String stu, PlainDate RegDate)
		{
			SchoolOid = SOid;
			studentOid = stu;
			RegistrationDate = RegDate;
			WithdrawDate = null;
			Membership_days = 0;
			Absences = 0;
			Tardy = 0;
			Unexcused = 0;
			Excused= 0;
			OSS = 0;
			ISS = 0;
		}

	}

}