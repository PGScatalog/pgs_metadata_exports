import sys, os.path, tarfile
import pandas as pd
import hashlib
#from catalog.models import *

class PGSExport:

    #---------------#
    # Configuration #
    #---------------#

    fields_to_include = {
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
                {'name': 'source_PMID', 'label': 'Source PubMed ID (PMID) or doi'},
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
                {'name': 'pub_id', 'label': 'PGS Publication (PGP) ID'},
                {'name': 'pub_pmid_label', 'label': 'Publication (PMID)'},
                {'name': 'pub_doi_label', 'label': 'Publication (doi)'},
                {'name': 'matches_publication', 'label': 'Score and results match the original publication'},
                {'name': 'ftp_scoring_file', 'label': 'FTP link'},
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
        'trait_label'
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
        'C-index': 'Corcordance Statistic (C-index)',
        other_metric_key: other_metric_label
    }

    def __init__(self,filename, data):
        self.filename = filename
        self.data = data
        self.pgs_list = []
        self.writer = pd.ExcelWriter(filename, engine='xlsxwriter')
        self.spreadsheets_conf = {
            'scores'     : ('Scores', self.create_scores_spreadsheet),
            'perf'       : ('Performance Metrics', self.create_performance_metrics_spreadsheet),
            'samplesets' : ('Evaluation Sample Sets', self.create_samplesets_spreadsheet),
            'samples_development': ('Score Development Samples', self.create_samples_development_spreadsheet),
            'publications': ('Publications', self.create_publications_spreadsheet),
            'efo_traits': ('EFO Traits', self.create_efo_traits_spreadsheet)
        }
        self.spreadsheets_list = [
            'scores', 'perf', 'samplesets', 'samples_development', 'publications', 'efo_traits'
        ]

    def set_pgs_list(self, pgs_list):
        """ List the PGS IDs used to generate the metadata files """
        if isinstance(pgs_list, list):
            self.pgs_list = pgs_list
        else:
            print('Error: '+str(pgs_list)+" is not a list")


    def save(self):
        """ Close the Pandas Excel writer and output the Excel file """
        self.writer.save()


    def generate_sheets(self, csv_prefix):
        """ Generate the differents sheets """

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
                self.generate_csv(data, csv_prefix, spreadsheet_label)
                print("CSV '"+spreadsheet_label+"' done")
            except:
                print("Issue to generate the spreadsheet '"+spreadsheet_label+"'")
                exit()


    def generate_sheet(self, data, sheet_name):
        """ Generate the Pandas dataframe and insert it as a spreadsheet into to the Excel file """
        try:
            # Create a Pandas dataframe.
            df = pd.DataFrame(data)
            # Convert the dataframe to an XlsxWriter Excel object.
            df.to_excel(self.writer, index=False, sheet_name=sheet_name)
        except NameError:
            print("Spreadsheet generation: At least one of the variables is not defined")
        except:
            print("Spreadsheet generation: There is an issue with the data of the spreadsheet '"+str(sheet_name)+"'")


    def generate_csv(self, data, prefix, sheet_name):
        """ Generate the Pandas dataframe and create a CSV file """
        try:
            # Create a Pandas dataframe.
            df = pd.DataFrame(data)
            # Convert the dataframe to an XlsxWriter Excel object.
            sheet_name = sheet_name.lower().replace(' ', '_')
            csv_filename = prefix+"_metadata_"+sheet_name+".csv"
            df.to_csv(csv_filename, index=False)
        except NameError:
            print("CSV generation: At least one of the variables is not defined")
        except:
            print("CSV generation: There is an issue with the data of the type '"+str(sheet_name)+"'")


    def generate_tarfile(self, output_filename, source_dir):
        """ Generate a tar.gz file from a directory """
        with tarfile.open(output_filename, "w:gz") as tar:
            tar.add(source_dir, arcname=os.path.basename(source_dir))


    def get_column_labels(self, classname, exception_field=None, exception_classname=None):
        """ Fetch the column labels from the Models """
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


    def create_md5_checksum(self, md5_filename='md5_checksum.txt', blocksize=4096):
        """ Returns MD5 checksum for the generated file. """

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
        """ Score spreadsheet """

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
            scores_data[score_labels['pub_id']].append(score['publication']['id'])
            scores_data[score_labels['pub_pmid_label']].append(score['publication']['PMID'])
            scores_data[score_labels['pub_doi_label']].append(score['publication']['doi'])
            # Mapped Traits
            trait_labels = []
            trait_ids = []
            for trait in score['trait_efo']:
                trait_labels.append(trait['label'])
                trait_ids.append(trait['id'])
            scores_data[score_labels['trait_label']].append(', '.join(trait_labels))
            scores_data[score_labels['trait_id']].append(', '.join(trait_ids))

            # Load the data into the dictionnary
            # e.g. column is "id":
            # `getattr` generates the perf.score method call
            # The following code is actually run:
            # scores_data[score_labels['id']].append(score.id)
            for column in score_labels.keys():
                if self.not_in_extra_fields_to_include(column):
                    value = score[column]
                    scores_data[score_labels[column]].append(value)
        return scores_data


    def create_performance_metrics_spreadsheet(self, pgs_list=[]):
        """ Performance Metrics spreadsheet """

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
            # Effect sizes
            effect_sizes_list = performance_metrics['effect_sizes']
            if effect_sizes_list:
                for metric in effect_sizes_list:
                    for label in metric['labels']:
                        if label in metrics_type:
                            m_header = metrics_header[label]
                            metrics_data[m_header] = metric['value']
            # Classification metrics
            class_acc_list = performance_metrics['class_acc']
            if class_acc_list:
                for metric in class_acc_list:
                    for label in metric['labels']:
                        if label in metrics_type:
                            m_header = metrics_header[label]
                            metrics_data[m_header] = metric['value']
            # Other metrics
            othermetrics_list = performance_metrics['othermetrics']
            if othermetrics_list:
                for metric in othermetrics_list:
                    m_label = metric['labels'][0]
                    m_data = m_label+" = "+metric['value']
                    if metrics_data[other_metric_label] == '':
                        metrics_data[other_metric_label] = m_data
                    else:
                        metrics_data[other_metric_label] = metrics_data[other_metric_label]+", "+m_data

            for m_header in list(metrics_header.values()):
                perf_data[m_header].append(metrics_data[m_header])

            # Load the data into the dictionnary
            for column in perf_labels.keys():
                if self.not_in_extra_fields_to_include(column):
                    value = perf[column]
                    perf_data[perf_labels[column]].append(value)
        return perf_data


    def create_samplesets_spreadsheet(self, pgs_list=[]):
        """ Sample Sets spreadsheet """

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

        #samplesets = list(samplesets)
        #samplesets.sort(key=lambda x: x['id'], reverse=False)

        for pss_id in sorted(samplesets.keys()):
            scores_ids = list(score_samplesets[pss_id])
            scores = ', '.join(sorted(scores_ids))

            pss = samplesets[pss_id]
            for sample in pss['samples']:
                object_data[sample_object_labels['associated_score']].append(scores)
                object_data[sample_object_labels['cohorts_list']].append(', '.join([c['name_short'] for c in sample['cohorts']]))

                for sample_column in sample_object_labels.keys():
                    if self.not_in_extra_fields_to_include(sample_column):
                        sample_value = sample[sample_column]
                        object_data[sample_object_labels[sample_column]].append(sample_value)
                        #sample_object_method_name = getattr(sample, sample_column)
                        #object_data[sample_object_labels[sample_column]].append(sample_object_method_name)

                for column in object_labels.keys():
                    if self.not_in_extra_fields_to_include(column):
                        value = pss[column]
                        object_data[object_labels[column]].append(value)
     
                        #object_method_name = getattr(pss, column)
                        #object_data[object_labels[column]].append(object_method_name)
        return object_data



    def create_samples_development_spreadsheet(self):
        """ Samples used for score development (GWAS and/or training) spreadsheet """

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
        for score in scores:
            for study_stage, stage_name in [('samples_variants', 'Source of Variant Associations (GWAS)'),
                                            ('samples_training','Score Development/Training')]:
                if study_stage == 'samples_variants':
                    samples = score['samples_variants']
                elif study_stage == 'samples_training':
                    samples = score['samples_training']

                if len(samples) > 0:
                    for sample in samples:
                        object_data[object_labels['associated_score']].append(score['id'])
                        object_data[object_labels['study_stage']].append(stage_name)

                        for column in object_labels.keys():
                            if self.not_in_extra_fields_to_include(column):
                                value = sample[column]
                                object_data[object_labels[column]].append(value)
                                #object_method_name = getattr(sample, column)
                                #object_data[object_labels[column]].append(object_method_name)

                        object_data[object_labels['cohorts_list']].append(', '.join([c['name_short'] for c in sample['cohorts']]))

        return object_data


    def create_publications_spreadsheet(self):
        """ Publications spreadsheet """

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

                # Performance publication
                score_performances = [ p for p in self.data['performance'] if p['associated_pgs_id'] == score['id'] ]
                #Performance.objects.filter(score=score)
                for score_perf in score_performances:
                    tmp_publication_ids.add(score_perf['publication']['id'])
            publications = [ x for x in self.data['publication'] if x['id'] in tmp_publication_ids ]
            publications.sort(key=lambda x: x['id'], reverse=False)

        for publi in publications:
            for column in object_labels.keys():
                if self.not_in_extra_fields_to_include(column):
                    value = publi[column]
                    object_data[object_labels[column]].append(value)
        return object_data


    def create_efo_traits_spreadsheet(self):
        """ EFO traits spreadsheet """

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
                    value = trait[column]
                    object_data[object_labels[column]].append(value)
        return object_data


    # def format_demographic_data(self, demographic):
    #     """ Combine the different elements of the Demographic model within 1 line """
    #     data = []
    #     if demographic:
    #         # Extract and format demographic data
    #         estimate = demographic.format_estimate()
    #         if not estimate:
    #             estimate = demographic.format_range()
    #         variability = demographic.format_variability()

    #         # Add formatted data
    #         if estimate:
    #             data.append(estimate)
    #         if variability:
    #             data.append(variability)
    #         if data:
    #             data.append(demographic.format_unit())
    #     if data:
    #         return ';'.join(data)
    #     return ''
