import argparse
import numpy as np
import pickle
import time
import torch
import torch.nn as nn
import torch.optim as optim
import transformer.Constants as Constants
import Utils
from preprocess.Dataset import get_dataloader
from transformer.Models import Transformer
from tqdm import tqdm
import random
import pandas as pd
import pytz
import glob
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
import os, sys
import warnings

warnings.filterwarnings("ignore")


def set_seed(seed):
	torch.backends.cudnn.deterministic = True
	torch.backends.cudnn.benchmark = False
	torch.manual_seed(seed)
	torch.cuda.manual_seed_all(seed)
	torch.use_deterministic_algorithms(True)
	np.random.seed(seed)
	random.seed(seed)


def prepare_dataloader(opt):
	""" Load data and prepare dataloader. """

	def load_data(name, dict_name):
		with open(name, 'rb') as f:
			data = pickle.load(f, encoding='latin-1')
			num_types = data['dim_process']
			data = data[dict_name]
			return data, int(num_types)

	#print('[Info] Loading train data...')
	train_data, num_types = load_data(opt.data + 'train.pkl', 'train')
	#print('[Info] Loading dev data...')
	dev_data, _ = load_data(opt.data + 'dev.pkl', 'dev')
	#print('[Info] Loading test data...')
	test_data, _ = load_data(opt.data + 'test.pkl', 'test')

	trainloader = get_dataloader(train_data, opt.batch_size, shuffle=False)
	testloader = get_dataloader(test_data, opt.batch_size, shuffle=False)
	return trainloader, testloader, num_types, len(train_data[0])


def train_epoch(model, training_data, optimizer, scheduler, pred_loss_func, opt, len_training_1):
	try:
		""" Epoch operation in training phase. """

		

		model.train()
		acc_1 = 0
		total_tries = 0
		val_loss_0 = 0
		val_loss_1 = 0
		total_acc_org = 0
		model_2 = 0
		for batch in tqdm(training_data, mininterval=1, desc='  - (Training)   ', leave=False):
			""" prepare data """
			training_lst = list(map(lambda x: x, batch))
			""" forward """
			optimizer.zero_grad()


			prediction, prediction_dir = model(training_lst)
			prediction = prediction.detach().numpy()
			#print(prediction)
			train = pd.DataFrame(pd.read_pickle('data/train.pkl').get('train')[0])
			columns_1 = train.columns.values[:-1]
			#columns_1_value = np.fromiter((float(c[11:]) for c in columns_1), dtype=float)
			columns_1_value = np.fromiter((float(c) for c in columns_1), dtype=float)
			
			'''
			prediction_col_tags_proba_1 = np.array([])
			for x in prediction:
				prediction_col_tags_proba_1 = np.append(prediction_col_tags_proba_1, np.argwhere(x==max(x, key=abs)).flatten()[0])
			prediction_col_tags_proba_1 = prediction_col_tags_proba_1.flatten().astype(int)
			prediction_col_tags_proba_2 = np.array([])
			for x in prediction:
				prediction_col_tags_proba_2 = np.append(prediction_col_tags_proba_2, max(x, key=abs))
			max_pred_1 = np.array([])
			for i in range(0, prediction_col_tags_proba_1.size):
				#max_pred_1 = np.append(max_pred_1, np.mean(np.multiply(columns_1_value[prediction_col_tags_proba_1[i]],prediction_col_tags_proba_2[i])))
				max_pred_1 = np.append(max_pred_1, np.mean(columns_1_value[prediction_col_tags_proba_1[i]]))
			
			'''
			prediction_col_tags_proba_1 = np.argmax(prediction, axis=1).flatten()

			max_pred_1 = np.array([])
			for x in prediction_col_tags_proba_1:
				max_pred_1 = np.append(max_pred_1, np.mean(columns_1_value[x]))
			#'''
			

			prediction_col_tags_proba = max_pred_1


			event_type_real_1 = train.type_event_final.values.flatten().astype(float)

			event_type_real_amount = event_type_real_1

			prediction_col_tags_proba_org = prediction_col_tags_proba
			event_type_real_1_org = event_type_real_1

			prediction_col_tags_proba = np.where(prediction_col_tags_proba>0, 1, 0)
			event_type_real_1 = np.where(event_type_real_1>0, 1, 0)

			pred_loss = np.argwhere(prediction_col_tags_proba!=event_type_real_1).flatten().astype(int).size/event_type_real_1.size
			correct_idx = np.argwhere(prediction_col_tags_proba==event_type_real_1).flatten().astype(int)
			incorrect_idx = np.argwhere(prediction_col_tags_proba!=event_type_real_1).flatten().astype(int)
			if correct_idx.size > 0:
				correct_sum = np.nansum(np.abs(event_type_real_amount[correct_idx]))
			else:
				correct_sum = 0
			if incorrect_idx.size > 0:
				incorrect_sum = np.nansum(np.abs(event_type_real_amount[incorrect_idx]))
			else:
				incorrect_sum = 0
			#return_training = correct_sum-incorrect_sum
			acc_1 += pred_loss
			total_tries += 1
			pred_loss = torch.tensor(pred_loss, requires_grad=True)
			acc_final = float(acc_1/total_tries)
			loss = pred_loss / total_tries
			loss_2 = pred_loss_func(torch.from_numpy(prediction_col_tags_proba_org).float(), torch.from_numpy(event_type_real_1_org).float())
			#loss_2 = torch.sum(loss_2)
			#val_loss_0 += loss_2
			#val_loss = val_loss_0/len_training_1
			val_loss_0 += correct_sum
			val_loss_1 += correct_sum+incorrect_sum
			val_loss = 1-(val_loss_0/val_loss_1)
			loss_2.requires_grad = True
			loss_2.backward()
			
			loss_3 = loss_2.detach().numpy()
			scheduler.step(loss_3)
			total_loss = loss_3
			total_acc = val_loss_0/val_loss_1
			if total_acc > total_acc_org:
				total_acc_org = total_acc
				model_2 = model

		if model_2 == 0:
			model_2 = model

		return model_2, total_acc_org, total_acc_org, loss_3, prediction_dir, event_type_real_1.size, prediction, columns_1_value
	except Exception as e:
		exc_type, exc_obj, exc_tb = sys.exc_info()
		fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
		print(exc_type, fname, exc_tb.tb_lineno)
		print(e)


def eval_epoch(model, validation_data, pred_loss_func, opt):
	try:
		""" Epoch operation in evaluation phase. """
		model.eval()
		total_event_ll = 0  # cumulative event log-likelihood
		total_time_se = 0  # cumulative time prediction squared-error
		total_event_rate = 0  # cumulative number of correct prediction
		total_num_event = 0  # number of total events
		total_num_pred = 0  # number of predictions
		with torch.no_grad():
			for batch in tqdm(validation_data, mininterval=1,desc='  - (Validation) ', leave=False):
				""" prepare data """
				#long, short, event_type = map(lambda x: x, batch)
				test_lst = list(map(lambda x: x, batch))

				""" forward """
				prediction, prediction_dir = model(test_lst)

		return prediction
	except Exception as e:
		exc_type, exc_obj, exc_tb = sys.exc_info()
		fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
		print(exc_type, fname, exc_tb.tb_lineno)
		print(e)


def train(model, training_data, validation_data, optimizer, scheduler_1, scheduler_2, pred_loss_func, opt, len_training_1):
	""" Start training. """

	try:
		total_acc_org = -100
		valid_event_losses = []  # validation log-likelihood
		valid_pred_losses = []  # validation event type prediction accuracy
		valid_rmse = []  # validation event time prediction RMSE
		for epoch_i in range(opt.epoch):
			try:
				epoch = epoch_i
				#print('[ Epoch', epoch, ']')
				
				start = time.time()
				model, acc_1, return_training_perc, val_loss, train_pred, training_size, training_proba, columns_1_value  = train_epoch(model, training_data, optimizer, scheduler_2, pred_loss_func, opt, len_training_1)
				#prediction  = eval_epoch(model, validation_data, pred_loss_func, opt)

				
				prediction = train_pred.detach().numpy()[-1].flatten()

				test_df = pd.DataFrame(pd.read_pickle('data/test.pkl').get('test')[0])
				result = test_df.type_event_final.values[0]

				#print(test_df.columns.values.size)

				#columns_1 = test_df.columns.values[:-1]
				#columns_1_value = np.fromiter((float(c[11:]) for c in columns_1), dtype=float)
				#columns_1_value = np.fromiter((float(c) for c in columns_1), dtype=float)
				#idx_0 = np.argwhere(columns_1_value!=0).flatten().astype(int)

				#columns_1_value = columns_1_value[idx_0]
				#prediction = prediction[idx_0]
				#print('check_me')
				#print(prediction)
				#print(columns_1_value)
				#max_proba_pred_0 = np.mean(np.multiply(columns_1_value[np.argmax(prediction).flatten()], prediction[np.argmax(prediction).flatten()]))
				max_proba_pred_1 = np.mean(columns_1_value[np.argmax(prediction).flatten()])

				prediction_col_tags_proba = np.array([0])

				if max_proba_pred_1 > 0:
					prediction_col_tags_proba = np.array([1])
				elif max_proba_pred_1 < 0:
					prediction_col_tags_proba = np.array([-1])

				prediction_pd = pd.DataFrame()
				prediction_pd['Predicted'] = pd.DataFrame(np.full(len(prediction_col_tags_proba),prediction_col_tags_proba[0]))
				prediction_pd.reset_index(drop=True, inplace=True)
				prediction_pd['pred_amount_abs'] = pd.DataFrame(np.full(len(prediction_pd),max_proba_pred_1))
				#prediction_pd['true_amount'] = pd.DataFrame(np.full(len(prediction_pd),result))
				prediction_pd['acc'] = pd.DataFrame(np.full(len(prediction_pd),return_training_perc))
				prediction_pd['proba'] = pd.DataFrame(np.full(len(prediction_pd),np.amax(prediction)))

				#prediction_pd_2 = prediction_pd
				#'''
				#total_acc = np.amax(prediction)
				total_acc = return_training_perc
				if total_acc > total_acc_org:
					total_acc_org = total_acc
			
					#torch.save(model.state_dict(), 'saved_models/model_'+str(epoch))
				
					prediction_pd_2 = prediction_pd

					if total_acc_org >= 1:
						break
				#'''

				scheduler_2.step(val_loss)
			except Exception as e:
				exc_type, exc_obj, exc_tb = sys.exc_info()
				fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
				print(exc_type, fname, exc_tb.tb_lineno)
				print(e)
		#with pd.option_context('display.max_rows', None, 'display.max_columns', None):
		#	print(prediction_pd_2)
		prediction_pd_2.to_csv('best_model_birthday.csv', mode='w', header=False)
	

	except Exception as e:
		exc_type, exc_obj, exc_tb = sys.exc_info()
		fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
		print(exc_type, fname, exc_tb.tb_lineno)
		print(e)


def main():
	""" Main function. """

	try:

		set_seed(0)

		parser = argparse.ArgumentParser()

		parser.add_argument('-data', required=True)

		parser.add_argument('-epoch', type=int, default=3)
		parser.add_argument('-batch_size', type=int, default=1)

		'''

		parser.add_argument('-d_model', type=int, default=2)
		parser.add_argument('-d_rnn', type=int, default=2)
		parser.add_argument('-d_inner_hid', type=int, default=1024)
		parser.add_argument('-d_k', type=int, default=512)
		parser.add_argument('-d_v', type=int, default=512)

		parser.add_argument('-n_head', type=int, default=4)
		parser.add_argument('-n_layers', type=int, default=4)
		
		'''

		parser.add_argument('-d_model', type=int, default=1)
		parser.add_argument('-d_rnn', type=int, default=1)
		parser.add_argument('-d_inner_hid', type=int, default=1)
		parser.add_argument('-d_k', type=int, default=1)
		parser.add_argument('-d_v', type=int, default=1)

		parser.add_argument('-n_head', type=int, default=1)
		parser.add_argument('-n_layers', type=int, default=1)

		#'''

		parser.add_argument('-dropout', type=float, default=0.1)
		parser.add_argument('-lr', type=float, default=1)
		parser.add_argument('-smooth', type=float, default=0.1)

		parser.add_argument('-log', type=str, default='log.txt')

		opt = parser.parse_args()

		opt.device = torch.device('cpu')

		with open(opt.log, 'w') as f:
			f.write('Epoch, Log-likelihood, Accuracy, RMSE\n')

		trainloader, testloader, num_types, len_training_1 = prepare_dataloader(opt)

		if len_training_1 == 0:
			dropout_1 = .5
		else:
			dropout_1 = 1/len_training_1
		if dropout_1 == 1:
			dropout_1 = 0

		model = Transformer(num_types=num_types, d_model=opt.d_model, d_rnn=opt.d_rnn, d_inner=opt.d_inner_hid, n_layers=opt.n_layers, n_head=opt.n_head, d_k=opt.d_k, d_v=opt.d_v, dropout=dropout_1)
		model.to(opt.device)

		optimizer = optim.Adam(filter(lambda x: x.requires_grad, model.parameters()), lr=dropout_1, betas=(0.9, 0.999), eps=0, amsgrad=False, maximize=False, weight_decay=0)
		scheduler_1 = optim.lr_scheduler.StepLR(optimizer=optimizer, step_size=1)
		scheduler_2 = optim.lr_scheduler.ReduceLROnPlateau(optimizer=optimizer, mode='min', patience=1, factor=dropout_1, threshold_mode='abs')
		
		#pred_loss_func = nn.PoissonNLLLoss(log_input=False, full=True, reduction='mean')
		pred_loss_func = nn.MSELoss(reduction='mean')
		#pred_loss_func = nn.BCEWithLogitsLoss(reduction='mean')

		num_params = sum(p.numel() for p in model.parameters() if p.requires_grad)


		train(model, trainloader, testloader, optimizer, scheduler_1, scheduler_2, pred_loss_func, opt, len_training_1)
		del model

	except Exception as e:
		exc_type, exc_obj, exc_tb = sys.exc_info()
		fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
		print(exc_type, fname, exc_tb.tb_lineno)
		print(e)

if __name__ == '__main__':
	main()