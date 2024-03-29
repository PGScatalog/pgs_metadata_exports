import sys, os.path, tarfile
import pandas as pd
import hashlib


#-----------------#
# Class PGSExport #
#-----------------#

class PGSExport:
    ''' Export each PGS metadata in different Excel file. '''

    #---------------#
    # Configuration #
    #---------------#

    fields_to_include = {
        'Cohort':
            [
                {'name': 'name_short', 'label': 'Cohort ID'},
                {'name': 'name_full', 'label': 'Cohort Name'},
                {'name': 'name_others', 'label': 'Previous/other/additional names'}
            ],
        'EFOTrait':
            [
                {'name': 'id', 'label': 'Ontology Trait ID'},
                {'name': 'label', 'label': 'Ontology Trait Label'},
                {'name': 'description', 'label': 'Ontology Trait Description'},
                {'name': 'url', 'label': 'Ontology URL'}
            ],
        'Sample':
            [
                {'name': 'associated_score', 'label': 'Polygenic Score (PGS) ID'},
                {'name': 'study_stage', 'label': 'Stage of PGS Development'},
                {'name': 'sample_number', 'label': 'Number of Individuals'},
                {'name': 'sample_cases', 'label': 'Number of Cases'},
                {'name': 'sample_controls', 'label': 'Number of Controls'},
                {'name': 'sample_percent_male', 'label': 'Percent of Participants Who are Male'},
                {'name': 'sample_age', 'label': 'Sample Age'},
                {'name': 'ancestry_broad', 'label': 'Broad Ancestry Category'},
                {'name': 'ancestry_free', 'label': 'Ancestry (e.g. French, Chinese)'},
                {'name': 'ancestry_country', 'label': 'Country of Recruitment'},
                {'name': 'ancestry_additional', 'label': 'Additional Ancestry Description'},
                {'name': 'phenotyping_free', 'label': 'Phenotype Definitions and Methods'},
                {'name': 'followup_time', 'label': 'Followup Time'},
                {'name': 'source_GWAS_catalog', 'label': 'GWAS Catalog Study ID (GCST...)'},
                {'name': 'source_PMID', 'label': 'Source PubMed ID (PMID)'},
                {'name': 'source_DOI', 'label': 'Source DOI'},
                {'name': 'cohorts_list', 'label': 'Cohort(s)'},
                {'name': 'cohorts_additional', 'label': 'Additional Sample/Cohort Information'}
            ],
        'SampleSet':
            [
                {'name': 'id', 'label': 'PGS Sample Set (PSS)'}
            ],
        'Score':
            [
                {'name': 'id', 'label': 'Polygenic Score (PGS) ID'},
                {'name': 'name', 'label': 'PGS Name'},
                {'name': 'trait_reported', 'label': 'Reported Trait'},
                {'name': 'trait_label', 'label': 'Mapped Trait(s) (EFO label)'},
                {'name': 'trait_id', 'label': 'Mapped Trait(s) (EFO ID)'},
                {'name': 'method_name', 'label': 'PGS Development Method'},
                {'name': 'method_params', 'label': 'PGS Development Details/Relevant Parameters'},
                {'name': 'variants_genomebuild', 'label': 'Original Genome Build'},
                {'name': 'variants_number', 'label': 'Number of Variants'},
                {'name': 'variants_interactions', 'label': 'Number of Interaction Terms'},
                {'name': 'weight_type', 'label': 'Type of Variant Weight'},
                {'name': 'pub_id', 'label': 'PGS Publication (PGP) ID'},
                {'name': 'pub_pmid_label', 'label': 'Publication (PMID)'},
                {'name': 'pub_doi_label', 'label': 'Publication (doi)'},
                {'name': 'matches_publication', 'label': 'Score and results match the original publication'},
                {'name': 'ancestry_gwas', 'label': 'Ancestry Distribution (%) - Source of Variant Associations (GWAS)'},
                {'name': 'ancestry_dev', 'label': 'Ancestry Distribution (%) - Score Development/Training'},
                {'name': 'ancestry_eval', 'label': 'Ancestry Distribution (%) - PGS Evaluation'},
                {'name': 'ftp_scoring_file', 'label': 'FTP link'},
                {'name': 'date_release', 'label': 'Release Date'},
                {'name': 'license', 'label': 'License/Terms of Use'}
            ],
        'Performance':
            [
                {'name': 'id', 'label': 'PGS Performance Metric (PPM) ID'},
                {'name': 'associated_pgs_id', 'label': 'Evaluated Score'},
                {'name': 'sampleset_id', 'label': 'PGS Sample Set (PSS)'},
                {'name': 'pub_id', 'label': 'PGS Publication (PGP) ID'},
                {'name': 'phenotyping_reported', 'label': 'Reported Trait'},
                {'name': 'covariates', 'label': 'Covariates Included in the Model'},
                {'name': 'performance_comments', 'label': 'PGS Performance: Other Relevant Information'},
                {'name': 'pub_pmid_label', 'label': 'Publication (PMID)'},
                {'name': 'pub_doi_label', 'label': 'Publication (doi)'}
            ],
        'Publication':
            [
                {'name': 'id', 'label': 'PGS Publication/Study (PGP) ID'},
                {'name': 'firstauthor', 'label': 'First Author'},
                {'name': 'title', 'label': 'Title'},
                {'name': 'journal', 'label': 'Journal Name'},
                {'name': 'date_publication', 'label': 'Publication Date'},
                {'name': 'date_release', 'label': 'Release Date'},
                {'name': 'authors', 'label': 'Authors'},
                {'name': 'doi', 'label': 'digital object identifier (doi)'},
                {'name': 'PMID', 'label': 'PubMed ID (PMID)'}
            ]
    }

    extra_fields_to_include = [
        'associated_score',
        'cohorts_list',
        'pub_doi_label',
        'pub_id',
        'pub_pmid_label',
        'sampleset_id',
        'study_stage',
        'trait_id',
        'trait_label',
        'ancestry_gwas',
        'ancestry_dev',
        'ancestry_eval'
    ]

    # Metrics
    other_metric_key = 'Other Metric'
    other_metric_label = other_metric_key+'(s)'
    metrics_type = ['HR','OR','β','AUROC','C-index',other_metric_label]
    metrics_header = {
        'HR': 'Hazard Ratio (HR)',
        'OR': 'Odds Ratio (OR)',
        'β': 'Beta',
        'AUROC': 'Area Under the Receiver-Operating Characteristic Curve (AUROC)',
        'C-index': 'Concordance Statistic (C-index)',
        other_metric_key: other_metric_label
    }

    # Data separator
    separator = '|'


    #-----------------#
    # General methods #
    #-----------------#

    def __init__(self, filename, data, ancestry_categories,pub_focused=None):
        self.filename = filename
        self.data = data
        self.ancestry_categories = ancestry_categories
        self.pub_focused = pub_focused
        self.pgs_list = []
        self.publication_ids = []
        self.writer = pd.ExcelWriter(filename, engine='xlsxwriter')

        # Order of the spreadsheets
        self.spreadsheets_list = [
            'publications', 'efo_traits', 'scores',
            'samples_development', 'perf', 'samplesets', 'cohorts'
        ]

        # Spreadsheets content creation
        self.spreadsheets_conf = {
            'scores'     : ('Scores', self.create_scores_spreadsheet),
            'perf'       : ('Performance Metrics', self.create_performance_metrics_spreadsheet),
            'samplesets' : ('Evaluation Sample Sets', self.create_samplesets_spreadsheet),
            'samples_development': ('Score Development Samples', self.create_samples_development_spreadsheet),
            'publications': ('Publications', self.create_publications_spreadsheet),
            'efo_traits' : ('EFO Traits', self.create_efo_traits_spreadsheet),
            'cohorts'    : ('Cohorts', self.create_cohorts_spreadsheet)
        }

        # Force data type in some columns
        # 'Int64' works better than "int" because it doesn't break if a non numeric value is found (e.g. missing PMID)
        self.spreadsheets_column_types = {
           'publications': { "PubMed ID (PMID)": 'Int64' },
           'scores': { "Publication (PMID)": 'Int64' },
           'perf': { "Publication (PMID)": 'Int64' },
           'samples_development': { "Source PubMed ID (PMID)": 'Int64' }
        }

    def set_pgs_list(self, pgs_list):
        ''' List the PGS IDs used to generate the metadata files '''
        if isinstance(pgs_list, list):
            self.pgs_list = pgs_list
        else:
            print('Error: '+str(pgs_list)+" is not a list")


    def save(self):
        ''' Close the Pandas Excel writer and output the Excel file '''
        self.writer.close()


    def generate_sheets(self, csv_prefix):
        ''' Generate the differents sheets '''

        if (len(self.spreadsheets_conf.keys()) != len(self.spreadsheets_list)):
            print("Size discrepancies between the dictionary 'spreadsheets' and the list 'spreadsheets_ordering'.")
            exit()
        if (csv_prefix == ''):
            print("CSV prefix, for the individual CSV spreadsheet is empty. Please, provide a prefix!")
            exit()

        for spreadsheet_name in self.spreadsheets_list:
            spreadsheet_label = self.spreadsheets_conf[spreadsheet_name][0]
            try:
                data = self.spreadsheets_conf[spreadsheet_name][1]()
                self.generate_sheet(data, spreadsheet_label)
                print("Spreadsheet '"+spreadsheet_label+"' done")
                self.generate_csv(data, csv_prefix, spreadsheet_name, spreadsheet_label)
                print("CSV '"+spreadsheet_label+"' done")
            except Exception as e:
                print(f'Issue to generate the spreadsheet "{spreadsheet_label}"\n> {e}')
                exit()


    def generate_sheet(self, data, sheet_label):
        ''' Generate the Pandas dataframe and insert it as a spreadsheet into to the Excel file '''
        try:
            # Create a Pandas dataframe.
            df = pd.DataFrame(data)
            # Convert the dataframe to an XlsxWriter Excel object.
            df.to_excel(self.writer, index=False, sheet_name=sheet_label)
        except NameError:
            print("Spreadsheet generation: At least one of the variables is not defined")
        except Exception as e:
            print(f'Spreadsheet generation: There is an issue with the data of the spreadsheet "{sheet_label}"\n> {e}')


    def generate_csv(self, data, prefix, sheet_name, sheet_label):
        ''' Generate the Pandas dataframe and create a CSV file '''
        try:
            # Create a Pandas dataframe.
            df = pd.DataFrame(data)
            # Force data type (e.g. issue with float for PubMed ID)
            if sheet_name in self.spreadsheets_column_types:
                df = df.astype(self.spreadsheets_column_types[sheet_name])
            # Convert the dataframe to an XlsxWriter Excel object.
            sheet_label = sheet_label.lower().replace(' ', '_')
            csv_filename = prefix+"_metadata_"+sheet_label+".csv"
            df.to_csv(csv_filename, index=False)
        except NameError:
            print("CSV generation: At least one of the variables is not defined")
        except Exception as e:
            print(f'CSV generation: There is an issue with the data of the type "{sheet_label}"\n> {e}')


    def generate_tarfile(self, output_filename, source_dir):
        ''' Generate a tar.gz file from a directory '''
        with tarfile.open(output_filename, "w:gz") as tar:
            tar.add(source_dir, arcname=os.path.basename(source_dir))


    def get_column_labels(self, classname, exception_field=None, exception_classname=None):
        ''' Fetch the column labels from the Models '''
        model_labels = {}
        for field in self.fields_to_include[classname]:
            field_name = field['name']
            label = field['label']
            model_labels[field_name] = label
        return model_labels


    def not_in_extra_fields_to_include(self,column):
        if column not in self.extra_fields_to_include:
            return True
        else:
            return False


    def cleanup_field_value(self,value):
        ''' Remove trailing characters (including new line). '''
        if isinstance(value, str):
            value = value.strip().replace('\n',' ').replace('\t',' ')
        return value


    def create_md5_checksum(self, md5_filename='md5_checksum.txt', blocksize=4096):
        ''' Returns MD5 checksum for the generated file. '''

        md5 = hashlib.md5()
        try:
            file = open(self.filename, 'rb')
            with file:
                for block in iter(lambda: file.read(blocksize), b""):
                    md5.update(block)
        except IOError:
            print('File \'' + self.filename + '\' not found!')
            return None
        except:
            print("Error: the script couldn't generate a MD5 checksum for '" + self.filename + "'!")
            return None

        md5file = open(md5_filename, 'w')
        md5file.write(md5.hexdigest())
        md5file.close()
        print("MD5 checksum file '"+md5_filename+"' has been generated.")


    #---------------------#
    # Spreadsheet methods #
    #---------------------#

    def create_scores_spreadsheet(self):
        ''' Score spreadsheet '''

        # Fetch column labels an initialise data dictionary
        score_labels = self.get_column_labels('Score')
        scores_data = {}
        for label in list(score_labels.values()):
            scores_data[label] = []

        scores = []
        if len(self.pgs_list) == 0:
            scores = self.data['score']
        else:
            scores = [ s for s in self.data['score'] if s['id'] in self.pgs_list ]

        for score in scores:
            
            # Publication
            publication = score['publication']
            scores_data[score_labels['pub_id']].append(publication['id'])
            scores_data[score_labels['pub_pmid_label']].append(publication['PMID'])
            scores_data[score_labels['pub_doi_label']].append(publication['doi'])
            
            # Mapped Traits
            trait_labels = []
            trait_ids = []
            for trait in score['trait_efo']:
                trait_labels.append(trait['label'])
                trait_ids.append(trait['id'])
            scores_data[score_labels['trait_label']].append(self.separator.join(trait_labels))
            scores_data[score_labels['trait_id']].append(self.separator.join(trait_ids))
            
            # Ancestries
            ancestries = score['ancestry_distribution']
            for stage in ('gwas','dev','eval'):
                ancestry_data = ''
                if stage in ancestries:
                    ancestry_datalist = []
                    for anc,val in sorted(ancestries[stage]['dist'].items(), key=lambda item: float(item[1]), reverse=True):
                        label = self.ancestry_categories[anc]
                        ancestry_datalist.append(f'{label}:{val}')
                    ancestry_data = self.separator.join(ancestry_datalist)
                scores_data[score_labels[f'ancestry_{stage}']].append(ancestry_data)

            # Load the data into the dictionnary
            # e.g. column is "id":
            # `getattr` generates the perf.score method call
            # The following code is actually run:
            # scores_data[score_labels['id']].append(score.id)
            for column in score_labels.keys():
                if self.not_in_extra_fields_to_include(column):
                    value = self.cleanup_field_value(score[column])
                    scores_data[score_labels[column]].append(value)
        return scores_data


    def create_performance_metrics_spreadsheet(self, pgs_list=[]):
        ''' Performance Metrics spreadsheet '''

        metrics_header = self.metrics_header
        metrics_type = self.metrics_type
        other_metric_label = self.other_metric_label

        # Fetch column labels an initialise data dictionary
        perf_labels = self.get_column_labels('Performance')
        perf_data = {}
        for label in list(perf_labels.values()):
            perf_data[label] = []

        # Addtional fields

        # Metrics
        for m_header in metrics_header:
            full_header = metrics_header[m_header]
            perf_data[full_header]  = []

        performances = []
        if len(self.pgs_list) == 0:
            performances = self.data['performance']
        else:
            score_performances = [ p for p in self.data['performance'] if p['associated_pgs_id'] in self.pgs_list ]
            for score_perf in score_performances:
                if score_perf not in performances:
                    performances.append(score_perf)
            performances.sort(key=lambda x: x['id'], reverse=False)

        for perf in performances:
            # Publication
            perf_publication = perf['publication']
            perf_data[perf_labels['pub_id']].append(perf_publication['id'])
            perf_data[perf_labels['pub_pmid_label']].append(perf_publication['PMID'])
            perf_data[perf_labels['pub_doi_label']].append(perf_publication['doi'])
            
            # SampleSet
            perf_data[perf_labels['sampleset_id']].append(perf['sampleset']['id'])

            # Metrics
            metrics_data = {}
            for m_header in list(metrics_header.values()):
                metrics_data[m_header] = ""

            performance_metrics = perf['performance_metrics']
            # Effect sizes and Classification metrics
            for metric_category in ('effect_sizes', 'class_acc'):
                metrics_list = performance_metrics[metric_category]
                if metrics_list:
                    for metric in metrics_list:
                        metric_name = metric['name_short']
                        if metric_name in metrics_type:
                            m_header = metrics_header[metric_name]
                            metrics_data[m_header] = metric['estimate']
                            if 'ci_lower' in metric:
                                metrics_data[m_header] = str(metrics_data[m_header])
                                metrics_data[m_header] += ' [{},{}]'.format(metric['ci_lower'],metric['ci_upper'])
                            elif 'se' in metric:
                                metrics_data[m_header] = str(metrics_data[m_header])
                                metrics_data[m_header] += ' ({})'.format(metric['se'])
            # Other metrics
            othermetrics_list = performance_metrics['othermetrics']
            if othermetrics_list:
                for metric in othermetrics_list:
                    m_label = metric['name_short']
                    m_data = m_label+" = "+str(metric['estimate'])
                    if 'ci_lower' in metric:
                        m_data += ' [{},{}]'.format(metric['ci_lower'],metric['ci_upper'])
                    elif 'se' in metric:
                        m_data += ' ({})'.format(metric['se'])
                    if metrics_data[other_metric_label] == '':
                        metrics_data[other_metric_label] = m_data
                    else:
                        metrics_data[other_metric_label] = metrics_data[other_metric_label]+", "+m_data

            for m_header in list(metrics_header.values()):
                perf_data[m_header].append(metrics_data[m_header])

            # Load the data into the dictionnary
            for column in perf_labels.keys():
                if self.not_in_extra_fields_to_include(column):
                    value = self.cleanup_field_value(perf[column])
                    perf_data[perf_labels[column]].append(value)
        return perf_data


    def create_samplesets_spreadsheet(self, pgs_list=[]):
        ''' Sample Sets spreadsheet '''

        # Fetch column labels an initialise data dictionary
        object_labels = self.get_column_labels('SampleSet')
        object_data = {}
        for label in list(object_labels.values()):
            object_data[label] = []

        # Sample
        sample_object_labels = self.get_column_labels('Sample')
        # Remove the "study_stage" column for this spreadsheet
        del sample_object_labels['study_stage']
        for label in list(sample_object_labels.values()):
            object_data[label] = []

        if len(self.pgs_list) == 0:
            performances = self.data['performance']
        else:
            # In this case, the Sample Sets / Score associations will be limited to the Score IDs from the provided list.
            performances = [ p for p in self.data['performance'] if p['associated_pgs_id'] in self.pgs_list ]
        
        samplesets = {}
        score_samplesets = {}
        for perf in performances:
            sampleset = perf['sampleset']
                
            pss_id = sampleset['id']
            score = perf['associated_pgs_id']
            if not pss_id in score_samplesets:
                score_samplesets[pss_id] = set()
            score_samplesets[pss_id].add(score)

            samplesets[pss_id] = sampleset

        for pss_id in sorted(samplesets.keys()):
            scores_ids = list(score_samplesets[pss_id])
            scores = ', '.join(sorted(scores_ids))

            pss = samplesets[pss_id]
            for sample in pss['samples']:
                object_data[sample_object_labels['associated_score']].append(scores)
                object_data[sample_object_labels['cohorts_list']].append(self.separator.join([c['name_short'] for c in sample['cohorts']]))

                for sample_column in sample_object_labels.keys():
                    if self.not_in_extra_fields_to_include(sample_column):
                        # Demographic data (not a simple key:value element)
                        if sample_column in ('sample_age','followup_time'):
                            demographic = sample[sample_column]
                            sample_value = None
                            if demographic:
                                sample_value = ''
                                if 'estimate' in demographic:
                                    sample_value += '{}:{}'.format(demographic['estimate_type'],demographic['estimate'])
                                if 'interval' in demographic:
                                    if sample_value != '':
                                        sample_value += ';'
                                    interval = demographic['interval']
                                    sample_value += '{}:[{},{}]'.format(interval['type'],interval['lower'],interval['upper'])
                                if 'variability' in demographic:
                                    if sample_value != '':
                                        sample_value += ';'
                                    sample_value += '{}:{}'.format(demographic['variability_type'],demographic['variability'])
                                if 'unit' in demographic:
                                    if sample_value != '':
                                        sample_value += ';'
                                    sample_value += 'unit:{}'.format(demographic['unit'])
                        else:
                            sample_value = self.cleanup_field_value(sample[sample_column])
                        object_data[sample_object_labels[sample_column]].append(sample_value)

                for column in object_labels.keys():
                    if self.not_in_extra_fields_to_include(column):
                        value = self.cleanup_field_value(pss[column])
                        object_data[object_labels[column]].append(value)
        return object_data



    def create_samples_development_spreadsheet(self):
        ''' Samples used for score development (GWAS and/or training) spreadsheet '''

        # Fetch column labels an initialise data dictionary
        object_labels = self.get_column_labels('Sample')
        object_data = {}
        for label in list(object_labels.values()):
            object_data[label] = []

        # Get the relevant scores
        if len(self.pgs_list) == 0:
             scores = self.data['score']
        else:
            scores = [ s for s in self.data['score'] if s['id'] in self.pgs_list ]

        #Loop through Scores to output their samples:
        score_studies = [
            ('samples_variants', 'Source of Variant Associations (GWAS)'),
            ('samples_training', 'Score Development/Training')
        ]
        for score in scores:
            for study_stage, stage_name in score_studies:
                samples = score[study_stage]

                if len(samples) > 0:
                    for sample in samples:
                        object_data[object_labels['associated_score']].append(score['id'])
                        object_data[object_labels['study_stage']].append(stage_name)

                        for column in object_labels.keys():
                            if self.not_in_extra_fields_to_include(column):
                                value = self.cleanup_field_value(sample[column])
                                object_data[object_labels[column]].append(value)

                        object_data[object_labels['cohorts_list']].append(self.separator.join([c['name_short'] for c in sample['cohorts']]))

        return object_data


    def create_publications_spreadsheet(self):
        ''' Publications spreadsheet '''

        # Fetch column labels an initialise data dictionary
        object_labels = self.get_column_labels('Publication')
        object_data = {}
        for label in list(object_labels.values()):
            object_data[label] = []

        publications = []
        if len(self.pgs_list) == 0:
            publications = self.data['publication']
        else:
            scores = [ s for s in self.data['score'] if s['id'] in self.pgs_list ]
            tmp_publication_ids = set()
            for score in scores:
                # Score publication
                tmp_publication_ids.add(score['publication']['id'])

                # Check if export focused on large studies
                if not self.pub_focused:
                    # Performance publication
                    score_performances = [ p for p in self.data['performance'] if p['associated_pgs_id'] == score['id'] ]
                    for score_perf in score_performances:
                        tmp_publication_ids.add(score_perf['publication']['id'])
                else:
                    self.publication_ids = tmp_publication_ids
            publications = [ x for x in self.data['publication'] if x['id'] in tmp_publication_ids ]
            publications.sort(key=lambda x: x['id'], reverse=False)

        for publi in publications:
            for column in object_labels.keys():
                if self.not_in_extra_fields_to_include(column):
                    value = self.cleanup_field_value(publi[column])
                    object_data[object_labels[column]].append(value)
        return object_data


    def create_efo_traits_spreadsheet(self):
        ''' EFO traits spreadsheet '''

        # Fetch column labels an initialise data dictionary
        object_labels = self.get_column_labels('EFOTrait')
        object_data = {}
        for label in list(object_labels.values()):
            object_data[label] = []

        traits = []
        if len(self.pgs_list) == 0:
            traits = self.data['trait']
        else:
            scores = [ s for s in self.data['score'] if s['id'] in self.pgs_list ]
            tmp_trait_ids = set()
            for score in scores:
                score_traits = score['trait_efo']
                for score_trait in score_traits:
                    tmp_trait_ids.add(score_trait['id'])
            traits = [ x for x in self.data['trait'] if x['id'] in tmp_trait_ids ]

        for trait in traits:
            for column in object_labels.keys():
                if self.not_in_extra_fields_to_include(column):
                    value = self.cleanup_field_value(trait[column])
                    object_data[object_labels[column]].append(value)
        return object_data


    def create_cohorts_spreadsheet(self):
        ''' Cohorts spreadsheet '''

        # Fetch column labels an initialise data dictionary
        object_labels = self.get_column_labels('Cohort')
        object_data = {}
        for label in list(object_labels.values()):
            object_data[label] = []

        cohorts = []
        if len(self.pgs_list) == 0:
            cohorts = self.data['cohort']
        else:
            scores = [ s for s in self.data['score'] if s['id'] in self.pgs_list ]
            tmp_cohort_ids = set()
            for score in scores:
                # Development cohorts
                for sample in ('samples_variants', 'samples_training'):
                    score_samples = score[sample]
                    for score_sample in score_samples:
                        for s_cohort in score_sample['cohorts']:
                            tmp_cohort_ids.add(s_cohort['name_short'])
                # Evaluation cohorts (via Performance Metrics and Sample Sets)
                performances = [ p for p in self.data['performance'] if p['associated_pgs_id'] == score['id'] ]

                for perf in performances:
                    sampleset = perf['sampleset']
                    for sample in sampleset['samples']:
                        for s_cohort in sample['cohorts']:
                            tmp_cohort_ids.add(s_cohort['name_short'])

                cohorts = [ x for x in self.data['cohort'] if x['name_short'] in tmp_cohort_ids ]

        for cohort in cohorts:
            for column in object_labels.keys():
                if self.not_in_extra_fields_to_include(column):
                    value = self.cleanup_field_value(cohort[column])
                    object_data[object_labels[column]].append(value)
        return object_data


#----------------------------#
# Class PGSExportAllMetadata #
#----------------------------#

class PGSExportAllMetadata(PGSExport):
    ''' Export all the PGS metadata in a unique Excel file. '''

    def create_readme_spreadsheet(self, release):
        ''' Info/readme spreadsheet '''

        readme_data = {}

        readme_data['PGS Catalog version'] = [release]
        readme_data['Number of Polygenic Scores'] = [len(self.data['score'])]
        readme_data['Number of Traits'] = [len(self.data['trait'])]
        readme_data['Number of Publications'] = [len(self.data['publication'])]

        df = pd.DataFrame(readme_data)
        df = df.transpose()
        df.to_excel(self.writer, sheet_name="Readme", header=False)

