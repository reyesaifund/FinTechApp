import math
import numpy as np
import torch
import torch.nn as nn
import torch.nn.functional as F
import transformer.Constants as Constants
from torch.nn import TransformerEncoder, TransformerEncoderLayer
import gc
import sys, os
from scipy.interpolate import splrep, splev
from scipy import stats
from sklearn.preprocessing import RobustScaler, MinMaxScaler, MaxAbsScaler
import random

def set_seed(seed):
	torch.backends.cudnn.deterministic = True
	torch.backends.cudnn.benchmark = False
	torch.manual_seed(seed)
	torch.cuda.manual_seed_all(seed)
	torch.use_deterministic_algorithms(True)
	np.random.seed(seed)
	random.seed(seed)

set_seed(0)

class LearnedSiLU(nn.Module):
	def __init__(self, slope=1):
		super().__init__()
		self.slope = slope * torch.nn.Parameter(torch.ones(1))

	def forward(self, x):
		try:
			x_org = x
			x_shape_0 = x_org.size(dim=0)
			x_shape_2 = x_org.size(dim=2)
			x = x.detach().numpy()
			#if x.size > 1:
			pred_final = np.array([])
			for x2 in x:
				#pred_final = np.append(pred_final, F.normalize(torch.FloatTensor(x2[0]), p=2, dim=0, eps=0).detach().numpy())
				pred_final = np.append(pred_final, x2[0])
			x = torch.FloatTensor(pred_final).reshape(x_shape_0, x_shape_2)
			#else:
			#	x = F.selu(F.normalize(torch.FloatTensor(x), p=2, dim=0)).reshape(x_org_shape)
			return x
		except Exception as e:
			exc_type, exc_obj, exc_tb = sys.exc_info()
			fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
			print(exc_type, fname, exc_tb.tb_lineno)
			print(e)

def get_non_pad_mask(seq):
	""" Get the non-padding positions. """
	return seq.ne(Constants.PAD).type(torch.float).unsqueeze(-1)

class RNN_layers(nn.Module):
	"""
	Optional recurrent layers. This is inspired by the fact that adding
	recurrent layers on top of the Transformer helps language modeling.
	"""

	def __init__(self, d_model, d_rnn, dropout, shape_1, shape_2):
		super().__init__()
		self.dropout_2 = dropout
		self.conv1 = nn.Conv2d(in_channels=1, out_channels=1, kernel_size=1, padding_mode='zeros', bias=False)
		self.conv2 = nn.Conv2d(in_channels=1, out_channels=1, kernel_size=1, padding_mode='zeros', bias=False)
		self.conv3 = nn.Conv2d(in_channels=1, out_channels=1, kernel_size=1, padding_mode='zeros', bias=False)
		self.bn_2 = nn.BatchNorm2d(1, momentum=int(1/self.dropout_2))
		self.pool1 = nn.MaxPool2d(kernel_size=1, ceil_mode=False)
		self.pool2 = nn.MaxPool2d(kernel_size=1, ceil_mode=False)
		self.pool3 = nn.MaxPool2d(kernel_size=1, ceil_mode=False)
		self.rnn = nn.LSTM(input_size=d_model, hidden_size=d_rnn, num_layers=1, bias=False, batch_first=True)
		self.projection = nn.Linear(d_rnn, d_model, bias=False)
		
		self.relu_1 = nn.ReLU()
		self.relu_2 = nn.ReLU()
		self.relu_3 = nn.ReLU()
		self.relu_4 = nn.LogSoftmax()
		#nn.init.normal_(self.projection.weight)
		#nn.init.kaiming_normal_(self.projection.weight, mode='fan_in', nonlinearity='relu')
		nn.init.xavier_uniform_(self.projection.weight)

		self.dropout = nn.FeatureAlphaDropout(p=dropout)
		#self.dropout = nn.AlphaDropout(p=dropout)
		#self.dropout = nn.Dropout(p=dropout)
		
		self.shape = shape_1

	def forward(self, enc_output):
		try:
			slope = LearnedSiLU()
			out = slope(enc_output)
			'''
			out = out.unsqueeze(0).unsqueeze(0)
			out = self.pool1(self.relu_1(self.conv1(out)))
			out = self.pool1(self.relu_1(self.conv1(out)))
			out = self.pool2(self.relu_2(self.conv2(out)))
			out = out.squeeze(0).squeeze(0)
			out = self.rnn(out)[0]
			'''
			#'''
			#out = out.unsqueeze(0).unsqueeze(0)
			dropout_3 = 0
			while dropout_3 <= 1:
				if dropout_3 >= 1:
					break
				out = out.unsqueeze(0).unsqueeze(0)
				out = self.bn_2(out)
				out = self.pool1(self.relu_1(self.conv1(out)))
				out = self.bn_2(out)
				out = self.pool1(self.relu_1(self.conv1(out)))
				out = self.bn_2(out)
				out = self.pool2(self.relu_2(self.conv2(out)))
				out = self.bn_2(out)
				out = out.squeeze(0).squeeze(0)
				out = self.rnn(out)[0]
				#out = self.bn_2(out)
				#out = self.projection(out)
				#out = torch.transpose(out, 0, 1)
				#out = self.bn_2(out)
				#out = self.relu_4(out)
				#out = torch.transpose(out, 0, 1)
				dropout_3 += self.dropout_2
			#'''
			#out = self.pool1(self.relu_1(self.conv1(out)))
			#out = self.pool2(self.relu_2(self.conv2(out)))
			#if list(out.size())[-1] > 1:
			#	out = self.bn_2(out)
			#out = self.pool3(self.relu_3(self.conv3(out)))
			#out = out.squeeze(0).squeeze(0)
			#out = self.pool3(self.relu(self.conv3(out)))
			#out = self.rnn(out)[0]
			#out = out.flatten(start_dim=1)
			#out = self.projection(out)
			#out = self.rnn(enc_output)[0].flatten(start_dim=1)
			#out = torch.transpose(out, 0, 1)
			#out = self.relu_4(out)
			#out = torch.transpose(out, 0, 1)
			#out = self.dropout(out)
			#'''
			#dropout_3 = 0
			#while dropout_3 <= 1:
			#	if dropout_3 >= 1:
			#		break
				#out = F.normalize(out, p=2, dim=0, eps=0)
			#	out = self.projection(out)
			#	out = self.relu_4(out)
			#	#out = self.dropout(out)
			#	dropout_3 += self.dropout_2
			#'''

			#out = self.projection(out)

			#out = torch.transpose(out, 0, 1)

			#out = self.projection(out)
			#out = self.relu_4(out)

			projection_2 = nn.Linear(list(out.size())[1], list(out.size())[1]+1, bias=False)
			out_2 = projection_2(out)
			
			#out_2 = out_2.flatten(start_dim=0)
			#out_2 = out_2.detach().numpy()

			out_2 = torch.transpose(out_2, 0, 1)

			out = self.projection(out)
			out = torch.transpose(out, 0, 1)
			out = self.relu_4(out)
			out_2 = self.relu_4(out_2)

			return out, out_2
		except Exception as e:
			exc_type, exc_obj, exc_tb = sys.exc_info()
			fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
			print(exc_type, fname, exc_tb.tb_lineno)
			print(e)


class Transformer(nn.Module):
	""" A sequence to sequence model with attention mechanism. """

	def __init__(
			self,
			num_types, d_model, d_rnn, d_inner,
			n_layers, n_head, d_k, d_v, dropout):
		super().__init__()

		self.num_types = num_types
		self.d_model = d_model
		self.d_rnn = d_rnn
		self.d_inner = d_inner
		self.n_layers = n_layers
		self.n_head = n_head
		self.d_k = d_k
		self.d_v = d_v
		self.dropout = dropout
		self.bn_2 = nn.BatchNorm2d(1, momentum=None)


		


		self.alpha = nn.Parameter(torch.tensor(-0.1))
		self.beta = nn.Parameter(torch.tensor(1.0))


	def forward(self, training_lst):

		try:
			#print(training_lst)
			training_dict = {k: [d.get(k) for d in training_lst] for k in set().union(*training_lst)}
			lst_1 = []
			for k, v in training_dict.items():
				if k != 'type_event_final':
					lst_1.append(torch.stack(v).float().flatten(start_dim=0))
			lst_1 = tuple(lst_1)
			enc_output = torch.stack(lst_1, 1).type(torch.LongTensor)
			#enc_output = F.normalize(enc_output, p=2, dim=0)
			#print('check')
			#print(enc_output.size())
			enc_output = torch.transpose(enc_output, 0, 1)
			#print(enc_output.size())
			#enc_output = enc_output.unsqueeze(0)
			#shape_1 = list(enc_output.size())
			#rnn = RNN_layers(enc_output.size(dim=1), enc_output.size(dim=1), self.dropout, shape_1)
			#enc_output = rnn(enc_output)[1]
			'''above is a 5 col and 5 row tensor structure'''
			'''below is supposed to be the weights'''
			#encoder = nn.Embedding.from_pretrained(enc_output)
			num_embeddings_1 = enc_output.size(dim=0)
			token_1 = enc_output.size(dim=1)
			#enc_output_idx = torch.arange(num_embeddings_1)
			encoder = nn.Embedding(token_1, token_1)
			enc_output_idx = encoder(enc_output)
			#print(enc_output_idx.size())
			'''<class 'IndexError'>'''
			#enc_output_idx = enc_output_idx * math.sqrt(token_1)
			#print(enc_output_idx.size())
			#pos_encoder = PositionalEncoding(token_1, self.dropout, token_1)
			#enc_output_idx = pos_encoder(enc_output_idx)
			#print(enc_output_idx.size())
			#enc_output_idx = torch.unsqueeze(enc_output_idx, 1)
			#encoder_layers = None
			encoder_layers = TransformerEncoderLayer(d_model=token_1, nhead=1, dropout=0, dim_feedforward=1, activation='relu')
			#n_head_try = 2
			#while encoder_layers is None:
			#	if n_head_try > 100:
			#		encoder_layers = TransformerEncoderLayer(d_model=token_1, nhead=1, dropout=0, dim_feedforward=1, activation='relu')
			#		break
			#	try:
			#		encoder_layers = TransformerEncoderLayer(d_model=token_1, nhead=n_head_try, dropout=0, dim_feedforward=1, activation='relu')
			#	except Exception as e:
			#		n_head_try += 1
			transformer_encoder = TransformerEncoder(encoder_layers, num_layers=1)
			enc_output_idx = transformer_encoder(src=enc_output_idx)
			#print(enc_output_idx.size())
			decoder = nn.Linear(token_1, token_1, bias=False)
			#nn.init.normal_(decoder.weight)
			nn.init.xavier_uniform_(decoder.weight)
			#nn.init.kaiming_normal_(decoder.weight, mode='fan_out', nonlinearity='leaky_relu')
			#enc_output_idx = decoder(enc_output_idx)
			#print(enc_output_idx.size())
			#print(enc_output_idx)
			shape_1 = list(enc_output_idx.size())
			rnn = RNN_layers(token_1, token_1, self.dropout, token_1, token_1)
			out, out_2 = rnn(enc_output_idx)

			return out, out_2

		except Exception as e:
			exc_type, exc_obj, exc_tb = sys.exc_info()
			fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
			print(exc_type, fname, exc_tb.tb_lineno)
			print(e)
			return 0, 0
