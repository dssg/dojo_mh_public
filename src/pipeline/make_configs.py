""" Format for dumped yaml file is slightly different than the hand-created one in their order but their
overall content is the same. Run file to create configs in the working directory.

1. Get default settings from base_config.yaml
2. Ammend the following settings
    a) selected_labels
    b) temporal parameters
    c) county
    d) model sets (i.e., random forest OR lin-reg, decision-tree, and baseline)
3. Write the amended config file to configs/ with name config_{county}_{model_set}_{label_tablename}.yaml
"""
import os
import yaml
from utils.helpers import get_label_tablename

NR_CORES = 85
COUNTIES = ['both', 'joco', 'doco']
CONFIGS_DIR = 'configs/' # Directory to store the config files
MODEL_SET_GROUP = ['model_sets_other', 'model_sets_rfs']

# All label groups we are considering
labels_dict = {
    'doco': [
        ['DEATH BY SUICIDE', 'DEATH BY OVERDOSE'],
        ['DEATH BY OVERDOSE', 'SUBSTANCE USE AMBULANCE RUN', 'DOCO DRUG DIAGNOSIS'],

        ['DEATH BY SUICIDE', 'SUICIDE ATTEMPT AMBULANCE RUN', 'SUICIDAL AMBULANCE RUN',
        'DOCO SUICIDE ATTEMPT DIAGNOSIS', 'DOCO SUICIDAL DIAGNOSIS'],

        ['DEATH BY SUICIDE', 'SUICIDE ATTEMPT AMBULANCE RUN', 'DOCO SUICIDE ATTEMPT DIAGNOSIS',
        'DEATH BY OVERDOSE', 'SUBSTANCE USE AMBULANCE RUN', 'DOCO DRUG DIAGNOSIS'],
        
        ['DEATH BY SUICIDE', 'SUICIDE ATTEMPT AMBULANCE RUN', 'DOCO SUICIDE ATTEMPT DIAGNOSIS',
        'SUICIDAL AMBULANCE RUN', 'DOCO SUICIDAL DIAGNOSIS', 
        'DEATH BY OVERDOSE', 'SUBSTANCE USE AMBULANCE RUN', 'DOCO DRUG DIAGNOSIS',
        'OTHER BEHAVIORAL CRISIS AMBULANCE RUN', 'DOCO OTHER MENTAL CRISIS DIAGNOSIS']
    ],

    'joco': [
        ['DEATH BY SUICIDE', 'DEATH BY OVERDOSE'],
        ['DEATH BY OVERDOSE', 'SUBSTANCE USE AMBULANCE RUN'],
        ['DEATH BY SUICIDE', 'SUICIDE ATTEMPT AMBULANCE RUN', 'SUICIDAL AMBULANCE RUN'],

        ['DEATH BY SUICIDE', 'SUICIDE ATTEMPT AMBULANCE RUN',
        'DEATH BY OVERDOSE', 'SUBSTANCE USE AMBULANCE RUN'],
        
        ['DEATH BY SUICIDE', 'SUICIDE ATTEMPT AMBULANCE RUN', 'SUICIDAL AMBULANCE RUN',
        'DEATH BY OVERDOSE', 'SUBSTANCE USE AMBULANCE RUN',
        'OTHER BEHAVIORAL CRISIS AMBULANCE RUN']
    ]
}

labels_dict['both'] = labels_dict['doco']

if __name__ == '__main__':

    # Config with settings that we do not wish to change
    BASE_CONFIG_PATH = 'config_templates/base_config.yaml'
    with open(BASE_CONFIG_PATH, 'r') as f:
        base_config = yaml.safe_load(f)

    if not os.path.exists(CONFIGS_DIR):
        os.mkdir(CONFIGS_DIR)

    for model_set_group in MODEL_SET_GROUP:
        for county in COUNTIES:
            labels = labels_dict[county]

            for label in labels:
                # Load model set
                template_name = f'{model_set_group}.yaml'
                with open(f'config_templates/{template_name}', 'r') as f:
                    model_set = yaml.safe_load(f)

                # Load temporal params
                temporal_name = f'temporal_{county}.yaml'
                with open(f'config_templates/{temporal_name}', 'r') as f:
                    temporal_params = yaml.safe_load(f)

                # Remove DoCo / JoCo feature from LinearRanker baseline
                if county != 'both' and 'LinearRanker' in model_set:
                    featuress = model_set['LinearRanker']['features']
                    weightss = model_set['LinearRanker']['weights']
                    for features, weights in zip(featuress, weightss):
                        if county == 'doco':
                            feat_idx = features.index('mhcserv_services_total_sum_9999m')
                        if county == 'joco':
                            feat_idx = features.index('dcservice_services_total_sum_9999m')

                        del features[feat_idx]
                        del weights[feat_idx]

                # Load default settings
                config = dict(base_config)

                # Remove the extended DoCo labels if only looking at JoCo
                if county == 'joco':
                    label = [x for x in label if 'DOCO' not in x]

                # Set specialized settings
                config['county'] = county
                config['labels']['selected_labels'] = label
                config['models'] = model_set
                config['temporal'] = temporal_params
                config['parallel'] = 0 if model_set_group in ['random-forests'] else 1
                config['nr_cores'] = NR_CORES

                # Get path to save in
                label_str = '-'.join(label)
                label_tablename = get_label_tablename(config)
                filename = f'config_{county}_{model_set_group}_{label_tablename}.yaml'
                path = os.path.join(CONFIGS_DIR, filename)

                with open(path, 'w') as f:
                    yaml.dump(config, f)
