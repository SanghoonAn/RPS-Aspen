import static com.follett.fsc.core.k12.business.ModelProperty.PATH_DELIMITER;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.commons.lang.time.DateUtils;
import org.apache.ojb.broker.query.Criteria;
import org.apache.ojb.broker.query.QueryByCriteria;
import org.jdom.JDOMException;

import com.follett.fsc.core.framework.persistence.SubQuery;
import com.follett.fsc.core.framework.persistence.X2Criteria;
import com.follett.fsc.core.k12.beans.QueryIterator;
import com.follett.fsc.core.k12.beans.RecordSet;
import com.follett.fsc.core.k12.beans.RecordSetKey;
import com.follett.fsc.core.k12.beans.Report;
import com.follett.fsc.core.k12.beans.SystemPreferenceDefinition;
import com.follett.fsc.core.k12.beans.X2BaseBean;
import com.follett.fsc.core.k12.business.PreferenceManager;
import com.follett.fsc.core.k12.business.StudentManager;
import com.follett.fsc.core.k12.tools.ToolBroker;
import com.follett.fsc.core.k12.tools.procedures.ProcedureJavaSource;
import com.follett.fsc.core.k12.tools.reports.ReportDataGrid;
import com.follett.fsc.core.k12.tools.reports.ReportJavaSourceNet;
import com.follett.fsc.core.k12.tools.reports.ReportUtils;
import com.follett.fsc.core.k12.web.UserDataContainer;
import com.follett.fsc.core.k12.web.nav.FilterException;
import com.x2dev.sis.model.beans.*;
import com.x2dev.sis.model.business.GraduationManager;
import com.x2dev.utils.types.PlainDate;

public class RPS_GRAD_V2 extends ReportJavaSourceNet
{
	private Criteria									m_studentCriteria;
	private SisStudent									m_currentStudent;
	private BigDecimal						            m_currentCredit	;		
	private ToolBroker 									m_broker;
	private String										m_report_type;
 
	private Map<String, Collection<GraduationRequirement>>		m_creditsNeeded;
	private Map<String, Map<String, Collection<SchoolCourse>>> m_inProgress; 
	private Map<String, Collection<GraduationRequirementHistory>>	m_gradRegHis;
	private Map<String , GraduationRequirement>         m_gradReqLookUp;
	private String	StandardDiploma		;	
	private GraduationManager   						m_graduationManager;
	private UserDataContainer    m_userData;

	private static final String[]						DIPLOMAS							=
		{ "STAY non-Diploma/Program", "ESOL", "Special Education Certificate - 24 CU", "STAY Career & Tech Certif.","DNU STAY test","DNU Banneker IB 27.5-CU-orig",
				"DNU1","DNU2","DNU3","DNU4","DNU Dunbar Pre-Eng","DNUBanneker CP thru 06-07","DNUBanneker IB 06-07 (1yr art)","DNUBanneker IB 28.5-CU",
				"IDEA","GED Prep","Options SHS 24-CU SY07-08+","Spec. Ed. Certificate" ,"DNU DPEHS 07-08+","DNUBanneker IB 06-07 (2yr art)"};

	private static final String							PRIMARY							="Primary";
	private static final String							ALT1							="Alt1";
	private static final String							ALT2							="Alt2"; 
	private static final String							QUERY_BY_PARAM					= "queryBy";
	private static final String							QUERY_STRING_PARAM				= "queryString";
	private static final String							SORT							= "studentSort";

	private static final String							REPORT_TYPE_PARAM				= "reportType";
	private static final String[]						GRADES							=
		{ "09", "10", "11", "12","13","14" };
	// Report Columns
	private static final String							SORT_BY					= "sort";
	private static final String							FIELD_STUDENT					= "student";
	private static final String							FIELD_PROGRAM					= "diploma";
	private static final String							FIELD_REQUIRED					= "req";
	private static final String							FIELD_NEED						= "need";
	private static final String							FIELD_EARNED					= "earned";
	private static final String							FIELD_VOC						= "voc";
	private static final String							FIELD_VOC_REQ					= "vocReq";
	private static final String							FIELD_POT_EARNED				= "potEarned";
	private static final String							FIELD_POT_NEED					= "potNeed";
	private static final String							FIELD_POT_VOC					= "potVoc";
	private static final String							SUMMER_CRED						= "summer";
	private static final String							COM_REQ							= "commReq";
	private static final String							COM_EARNED						= "commEarned";
	private static final String							EIGHT_MONTHS					= "eight";
	private static final String							REPORT_TYPE						= "ReportType";
	private static final String							SUMMER_SCHOOL					= "Summer School";


	/**
	 * @see com.follett.fsc.core.k12.tools.reports.ReportJavaSourceNet#gatherData()
	 */
	@Override
	protected Object gatherData()  
	{
		m_graduationManager = new GraduationManager(getBroker());
		Criteria dCriteria = new Criteria();		
        dCriteria.addEqualTo(GraduationProgram.COL_NAME , "Standard Diploma");
        QueryByCriteria dQuery = new QueryByCriteria(GraduationProgram.class , dCriteria);
        GraduationProgram diploma = (GraduationProgram) getBroker().getBeanByQuery(dQuery);
        
        StandardDiploma = diploma.getOid() ;
        
        buildCredits();
        m_inProgress = new HashMap<String, Map<String, Collection<SchoolCourse>>>();
		ReportDataGrid grid = new ReportDataGrid();
		m_broker = (ToolBroker) getBroker();
		String sortString = null;
		buildCriteria();
		m_report_type = (String) getParameter(REPORT_TYPE_PARAM); 
		if (m_report_type ==null)
		{
			m_report_type = "p";
		}
		addParameter(REPORT_TYPE, m_report_type);
		String m_sort =  (String) getParameter(SORT);

		QueryByCriteria stuQuery = new QueryByCriteria(SisStudent.class, m_studentCriteria);
		stuQuery.setDistinct(Boolean.TRUE);
		m_sort = "1";
		switch (m_sort)
		{
		case "1" : stuQuery.addOrderByAscending(SisStudent.COL_NAME_VIEW); sortString = "Name"; break;
		case "2" : stuQuery.addOrderByAscending(SisStudent.COL_LOCAL_ID); sortString = "Student ID";  break; 
		case "4" : stuQuery.addOrderByAscending(SisStudent.COL_GRADE_LEVEL); sortString = "Grade";  break;
		default : stuQuery.addOrderByAscending(SisStudent.COL_NAME_VIEW);  sortString = "Name";  

		}

		addParameter(SORT_BY, sortString);

		Collection<SisStudent> Students = m_broker.getCollectionByQuery(stuQuery);
		m_gradRegHis = new HashMap<String, Collection<GraduationRequirementHistory>>();
		QueryIterator stu = m_broker.getIteratorByQuery(stuQuery);
		 
			processGradReqHistory(stu);
		 
		SubQuery stuSub = new SubQuery(SisStudent.class, X2BaseBean.COL_OID, m_studentCriteria);
  
		PlainDate Today = new PlainDate(DateUtils.truncate(new Date(), java.util.Calendar.DAY_OF_MONTH));

		Criteria gradCriteria = new Criteria();
		gradCriteria.addIn( GraduationRequirementHistory.COL_STUDENT_OID, stuSub);
		gradCriteria.addGreaterOrEqualThan(GraduationRequirementHistory.COL_RUN_DATE , Today);
		QueryByCriteria gradtQuery = new QueryByCriteria(GraduationRequirementHistory.class, gradCriteria); 	 
		m_gradRegHis = getBroker().getGroupedCollectionByQuery(gradtQuery, GraduationRequirementHistory.COL_STUDENT_OID, 100);


		//ReportDataGrid StuGrid = new ReportDataGrid();
		for (SisStudent student : Students)
		{ 
			processStudent(student, grid); 
		}
		//grid.append();
		//grid.set(STUDENT_GRID, StuGrid);
		grid.beforeTop();
		return  grid;

	}



	/**
	 * Builds the <code>m_transcriptCriteria</code> and <code>m_studentCriteria</code> objects based on user input.
	 */
	private void buildCriteria()
	{ 
		List<String> studentGrades = new ArrayList<String>(Arrays.asList(GRADES));
		/*
		 * Build a criteria that will find the students to calculate attendance and credit totals for the student.
		 */
		m_studentCriteria = new Criteria();
		//m_studentCriteria.addIn(SisStudent.COL_GRADE_LEVEL, studentGrades);

		if (m_currentStudent != null)
		{
			/*
			 * Running for one student
			 */
			m_studentCriteria.addEqualTo(X2BaseBean.COL_OID, m_currentStudent.getOid());
		}
		else
		{
			if (isSchoolContext())
			{
				m_studentCriteria.addEqualTo(SisStudent.COL_SCHOOL_OID, ((SisSchool) getSchool()).getOid());
			}

			String activeCode = PreferenceManager.getPreferenceValue(getOrganization(),
					SystemPreferenceDefinition.STUDENT_ACTIVE_CODE);
			m_studentCriteria.addEqualTo(SisStudent.COL_ENROLLMENT_STATUS, activeCode);

			String queryBy = (String) getParameter(QUERY_BY_PARAM).toString() ; 

			switch (queryBy)
			{
			case "100": // Current selection
				m_studentCriteria = getCurrentCriteria();
				break; 
			default:            
				// No additional criteria (this is the case for "All")
				break;
			} 
			//	addUserCriteria(m_studentCriteria, queryBy, queryString, null, null);
		}
	}

	/**
	 * Builds the <code>m_creditsNeeded</code> and <code>m_oidtoName</code> objects 
	 */
	private void buildCredits()
	{
		String LastProgram = null;
		String CurrentProgram = null;
		Map<String, BigDecimal> m_reqSet = new HashMap<String, BigDecimal>(); 
		Criteria criteria = new Criteria();
		QueryByCriteria gradReqQuery = new QueryByCriteria(GraduationRequirement.class, criteria);
		gradReqQuery.addOrderByAscending(GraduationRequirement.COL_PROGRAM_STUDIES_OID );
		gradReqQuery.addOrderBy(GraduationRequirement.COL_PARENT_REQUIREMENT_OID, true);
		gradReqQuery.addOrderBy(GraduationRequirement.COL_SUB_PROGRAM_STUDIES_OID , false); 
		
		Object[] gradReqs = getBroker().getCollectionByQuery(gradReqQuery).toArray();
		
		m_gradReqLookUp  = getBroker().getMapByQuery(gradReqQuery, GraduationRequirement.COL_OID, 200);
		m_creditsNeeded  = getBroker().getGroupedCollectionByQuery(gradReqQuery, GraduationRequirement.COL_PROGRAM_STUDIES_OID,    5);
  

	}
	
	
	private void processStudent(  SisStudent student , final ReportDataGrid grid)	
	{
		String ProgramOfStudy; 	  
		m_currentCredit = BigDecimal.ZERO;
		BigDecimal m_CreditNeeded = BigDecimal.ZERO;
		BigDecimal m_CreditReq = BigDecimal.ZERO;
		BigDecimal m_voc = BigDecimal.ZERO;
		BigDecimal m_vocReq = BigDecimal.ZERO; 
		BigDecimal m_POTNeeded = BigDecimal.ZERO;
		BigDecimal m_POTCredit = BigDecimal.ZERO;
		 
		int YOG = student.getYog() ;

/*
		if (ninth != null && ninth.indexOf("2") >0)
		{
			ninthYear = Integer.parseInt( ninth.substring(ninth.indexOf("2"), ninth.indexOf("2")+4));
		} 
*/
		int yogYear = getOrganization().getCurrentContext().getSchoolYear();
		
		if (m_report_type.equals("t") ) yogYear++;
			
		
		if (YOG <= ( yogYear ) && student.getEnrollmentStatus().equals("Active"))
		{  
			grid.append();
			grid.set(FIELD_STUDENT, student); 
			
			if (student.getProgramStudies() == null || student.getProgramStudies().isEmpty())
			{
				ProgramOfStudy = StandardDiploma;
			}
			else
			{
				ProgramOfStudy = student.getProgramStudies().iterator().next().getProgramStudiesOid(); 
			}   
		
			
			GraduationProgram pr = (GraduationProgram) getBroker().getBeanByOid(GraduationProgram.class, ProgramOfStudy);
			 
				int arraySize ;
				if (m_creditsNeeded.containsKey(ProgramOfStudy))
				{
					arraySize =m_creditsNeeded.get(ProgramOfStudy).size();

				}
				else {
					arraySize =1;
				} 
				 
				Map<String, GraduationRequirementHistory> lastRuns = new HashMap<String, GraduationRequirementHistory>();
				Collection <GraduationRequirementHistory> gradReqs=		 m_gradRegHis.get(student.getOid());
				if(gradReqs !=null &&   !gradReqs.isEmpty())
				{

					for (GraduationRequirementHistory temp : gradReqs )
					{
						
						if (temp.getProgramOid().equals(ProgramOfStudy))
						if (lastRuns.containsKey(temp.getRequirementOid())  )
						{
							if (lastRuns.get(temp.getRequirementOid()).getRunDate().compareTo(temp.getRunDate()) <=0 )
							{
								lastRuns.put(temp.getRequirementOid(), temp);
							}
						}
						else
						{
							lastRuns.put(temp.getRequirementOid(), temp);
						}
					
					}

 

						// Process through all requirements
					if (m_creditsNeeded.containsKey(ProgramOfStudy))
					{
						for (GraduationRequirement category : m_creditsNeeded.get(ProgramOfStudy) )
						{ 
							if (category.getParentRequirementOid() == null)
							{
								BigDecimal earned = BigDecimal.ZERO;
								BigDecimal deficient = BigDecimal.ZERO;

								if ( lastRuns.containsKey(category.getOid()) )
								{
									earned = lastRuns.get(category.getOid()).getUnitGained();
									earned =earned.add( lastRuns.get(category.getOid()).getUnitWaived());
								}
								deficient =  category.getRequiredUnit().subtract(earned ); 

								// zero it out if negative
								if (deficient.signum() == -1)
								{
									deficient = BigDecimal.ZERO;
								}
								// fill in grid
								 
									m_CreditNeeded = m_CreditNeeded.add(deficient);
									m_currentCredit = m_currentCredit.add(earned);
									m_CreditReq = m_CreditReq.add( category.getRequiredUnit());
									String VID = category.getOid();
									GraduationRequirementHistory LR = lastRuns.get(VID);
									if (LR != null)
										{BigDecimal UIP = LR.getUnitInProgress();
									m_POTCredit = m_POTCredit.add(UIP.add(earned) );}
								 
							}
 
						}
					}

					grid.set(FIELD_PROGRAM, pr.getName());
					grid.set(FIELD_EARNED, m_currentCredit);
					grid.set(FIELD_NEED, m_CreditNeeded);
					grid.set(FIELD_REQUIRED, m_CreditReq);
					grid.set(FIELD_VOC, m_voc);
					grid.set(FIELD_VOC_REQ, m_vocReq);
					grid.set(FIELD_POT_EARNED, m_POTCredit);
					grid.set(FIELD_POT_NEED, m_CreditReq.subtract(m_POTCredit));
  
				}
			 
		}
	}

	private BigDecimal getVocCredits ( String Program_name, GraduationProgram Program)
	{

		BigDecimal cred = null; 
		Collection<GraduationRequirement> gradReqs  = Program.getRequirements();


		for ( GraduationRequirement gradReq : gradReqs)
		{
			GraduationRequirement gradReqEntry = (GraduationRequirement) gradReq;  

			cred = gradReqEntry.getRequiredUnit() ; 

		} 
		return cred;
	}	
 

	   private void processGradReqHistory(QueryIterator iterator) 
	    {
	    	ReportDataGrid grid = new ReportDataGrid();
	        /*
	         * Get a map of the courses with partial credit course requirements.
	         */
	    	String programStudiesOid;
	        
	       
	        String activeCode = PreferenceManager.getPreferenceValue(getOrganization(), SystemPreferenceDefinition.STUDENT_ACTIVE_CODE);

	        m_graduationManager = new GraduationManager(getBroker());
 
	        //criteria.addEqualTo(SisStudent.COL_LOCAL_ID, "9224653");//ADDED THIS AT DCPS - AUG 25 2015
	        /*
	         * Get a map of the courses with partial credit course requirements.
	         */

	        try
	        {
	            while (iterator.hasNext())
	            {
	                SisStudent student = (SisStudent) iterator.next();

	                if (student.getProgramStudies().isEmpty())
	    	        {
	    	        	programStudiesOid = StandardDiploma;
	    	        }
	    	        else
	    	        {
	    	        	programStudiesOid = student.getProgramStudies().iterator().next().getProgramStudiesOid();
	    	        } 
	    	        
	    	        
	    	        X2Criteria partialCourseReqCriteria = new X2Criteria();
	    	        partialCourseReqCriteria.addEqualTo(GraduationCourseRequirement.REL_REQUIREMENT + "." + GraduationRequirement.COL_PROGRAM_STUDIES_OID, programStudiesOid);
	    	        partialCourseReqCriteria.addNotEqualTo(GraduationCourseRequirement.COL_PARTIAL_CREDIT, new Double("0.0"));

	    	        QueryByCriteria partialQuery = new QueryByCriteria(GraduationCourseRequirement.class, partialCourseReqCriteria);
	    	        
	    	        Map<String, List<GraduationCourseRequirement>> partialCourseRequirments = getBroker().getGroupedCollectionByQuery(partialQuery, GraduationCourseRequirement.COL_COURSE_OID, 100);

	                HashMap<String, List<SchoolCourse>> coursesGainedCredit = new HashMap<String, List<SchoolCourse>>();
	                HashMap<String, List<SchoolCourse>> coursesTaken = new HashMap<String, List<SchoolCourse>>();
	                HashMap<String, List<SchoolCourse>> coursesTaking = new HashMap<String, List<SchoolCourse>>();
	                HashMap<String, Double>             creditsGained = new HashMap<String, Double>();
	                HashMap<String, Double>             rawCreditsGained = new HashMap<String, Double>();
	                HashMap<String, Double>             creditsWaived = new HashMap<String, Double>();
	                HashMap<String, Double>             creditsRequired = new HashMap<String, Double>();
	                HashMap<String, Double>             creditsByCourse = new HashMap<String, Double>();
	                HashMap<String, Double>             creditsInProgress = new HashMap<String, Double>();
	                HashMap<String, String>             gradeLevelByCourse = new HashMap<String, String>();
	                Map<String, Map<String, Object>>    otherRequirementValues = new HashMap<String, Map<String, Object>>();
	                List<String>                        satisfiedOtherRequirementOids = new ArrayList<String>();
                 try {
						m_graduationManager.determineGraduationStatus(student,
						                                              m_userData,
						                                              programStudiesOid,
						                                              coursesGainedCredit,
						                                              coursesTaken,
						                                              coursesTaking,
						                                              new HashMap<String, List<SchoolCourse>>(),
						                                              new HashMap<String, List<String>>(),
						                                              creditsGained,
						                                              rawCreditsGained,
						                                              creditsWaived,
						                                              creditsRequired,
						                                              creditsByCourse,
						                                              creditsInProgress,
						                                              new HashMap<String, Double>(),
						                                              gradeLevelByCourse,
						                                              false,
						                                              partialCourseRequirments,
						                                              new HashMap<String, Map<String, String>>(),
						                                              otherRequirementValues,
						                                              satisfiedOtherRequirementOids);
					} catch (FilterException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (JDOMException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}   catch (NullPointerException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
 
 

	                String totalCompleted = "";
	                double totalWaived = 0;
	                double status = 0;
	                double totalInProgress = 0;
	                double required = 0;

	                GraduationProgram GR = (GraduationProgram) getBroker().getBeanByOid(GraduationProgram.class,programStudiesOid );
	                Collection<GraduationRequirement> programRequirements  = GR.getRequirements();
	                
	                for (GraduationRequirement r : programRequirements)
	                {
	                    totalCompleted = "";
	                    totalWaived = 0;
	                    status = 0;
	                    totalInProgress = 0;
	                    required = 0;
	                    String requirementOid = r.getOid();

	                    System.out.println("Requirement Description - " + r.getDescription() + "Requirement Oid - " + requirementOid);
	                    if("CSH".equals(r.getCode()))
	                    {
	                        continue;
	                    }

	                    status = m_graduationManager.getRequirementSatisfiedStatus(requirementOid, creditsGained,
	                            creditsWaived, creditsRequired, satisfiedOtherRequirementOids);
	                  /*  if (otherRequirementValues.get(requirementOid) != null && !otherRequirementValues.get(requirementOid).isEmpty())
	                    {
	                        Map<String, Object> otherValues = otherRequirementValues.get(requirementOid);

	                        for (String key : otherValues.keySet())
	                        {
	                            if (otherValues.get(key) instanceof String)
	                            {
	                                String[] values = ((String)otherValues.get(key)).split(":");
	                                totalCompleted = totalCompleted.concat(values[1] + " ");
	                            }
	                        }
	                    }
	                    else
	                    { */
	                        totalCompleted = String.valueOf(m_graduationManager.getTotalCreditsGained(null, requirementOid, creditsGained));
	                    //}
	                    totalWaived = m_graduationManager.getTotalWaiver(null, requirementOid, creditsWaived);
	                    totalInProgress = 0;
	                    if (coursesTaking.get(requirementOid) != null)
	                    {
	                        for (SchoolCourse course : coursesTaking.get(requirementOid))
	                        {
	                        	if (!m_inProgress.containsKey(student.getOid())) m_inProgress.put(student.getOid(), new HashMap<String, Collection<SchoolCourse>>());
	          	              
	                        	if (!m_inProgress.get(student.getOid()).containsKey(requirementOid)) m_inProgress.get(student.getOid()).put(requirementOid, new HashSet<SchoolCourse>());
	                        	
	                        	m_inProgress.get(student.getOid()).get(requirementOid).add(course);
	                        	
	                            totalInProgress += course.getCredit().doubleValue();
	                        }
	                    }
	                    required = m_graduationManager.getTotalRequired(null, requirementOid, creditsRequired);

	                    
	                        GraduationRequirementHistory history = (GraduationRequirementHistory) X2BaseBean.newInstance(GraduationRequirementHistory.class, getBroker().getPersistenceKey());
	                        history.setSchoolOid(student.getSchoolOid());
	                        history.setStudentOid(student.getOid());
	                        history.setContextOid(getOrganization().getCurrentContextOid());
	                        history.setRequirementOid(requirementOid);
	                        history.setProgramOid(programStudiesOid);
	                        history.setRunDate(new PlainDate());
	                        history.setStatus(new BigDecimal(status));
	                        history.setUnitGained(new   BigDecimal(totalCompleted));
	                        history.setUnitInProgress(new BigDecimal(totalInProgress));
	                        history.setUnitRequired(new BigDecimal(required));
	                        history.setUnitWaived(new BigDecimal(totalWaived));
	                        getBroker().saveBeanForced(history);
	                     
	                }
	            }

	        }
	        finally
	        {
	            if (iterator != null)
	            {
	                iterator.close();
	            }
	        }
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
	        m_currentStudent = (SisStudent) userData.getCurrentRecord(SisStudent.class);
	         m_userData = userData; 
	    }
	    
}