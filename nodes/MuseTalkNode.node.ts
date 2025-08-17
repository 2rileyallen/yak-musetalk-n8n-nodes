import {
	IExecuteFunctions,
	INodeExecutionData,
	INodeType,
	INodeTypeDescription,
	NodeConnectionType,
	NodeOperationError,
	IHttpRequestOptions,
	IDataObject,
} from 'n8n-workflow';
import WebSocket from 'ws';
import * as path from 'path';
import { promises as fs } from 'fs';
import { tmpdir } from 'os';

export class MuseTalkNode implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'MuseTalk',
		name: 'museTalkNode',
		icon: 'file:museTalkNode.svg',
		group: ['transform'],
		version: 1,
		description: 'Uses MuseTalk to generate lip-synced videos from audio and video inputs.',
		defaults: {
			name: 'MuseTalk',
		},
		inputs: [NodeConnectionType.Main],
		outputs: [NodeConnectionType.Main],
		properties: [
			// ----------------------------------
			//         Output Settings
			// ----------------------------------
			{
				displayName: '--- Output Settings ---',
				name: 'outputSettingsNotice',
				type: 'notice',
				default: '',
			},
			{
				displayName: 'Output as File Path',
				name: 'outputAsFilePath',
				type: 'boolean',
				default: true,
				description: 'Whether to return the output as a file path. Uncheck to return as binary data.',
			},
			{
				displayName: 'Output File Path',
				name: 'outputFilePath',
				type: 'string',
				default: '',
				displayOptions: {
					show: {
						outputAsFilePath: [true],
					},
				},
				placeholder: '/path/to/output.mp4',
				description: 'The full file path to save the output video. The gatekeeper service must have write access to this location.',
				required: true,
			},
			{
				displayName: 'Output Binary Property Name',
				name: 'outputBinaryPropertyName',
				type: 'string',
				default: 'data',
				displayOptions: {
					show: {
						outputAsFilePath: [false],
					},
				},
				description: 'The name to give the output binary property for the generated video.',
			},

			// ----------------------------------
			//         Input Audio
			// ----------------------------------
			{
				displayName: '--- Input Audio ---',
				name: 'inputAudioNotice',
				type: 'notice',
				default: '',
			},
			{
				displayName: 'Use File Path for Audio',
				name: 'audioUseFilePath',
				type: 'boolean',
				default: true,
				description: 'Use a file path for the input audio. Uncheck to use binary data from a previous node.',
			},
			{
				displayName: 'Audio File Path',
				name: 'audioFilePath',
				type: 'string',
				default: '',
				displayOptions: {
					show: {
						audioUseFilePath: [true],
					},
				},
				placeholder: '/path/to/audio.wav',
				description: 'The full file path to the input audio file.',
				required: true,
			},
			{
				displayName: 'Audio Binary Property Name',
				name: 'audioBinaryPropertyName',
				type: 'string',
				default: 'data',
				displayOptions: {
					show: {
						audioUseFilePath: [false],
					},
				},
				description: 'The name of the binary property containing the input audio data.',
				required: true,
			},

			// ----------------------------------
			//         Input Video
			// ----------------------------------
			{
				displayName: '--- Input Video ---',
				name: 'inputVideoNotice',
				type: 'notice',
				default: '',
			},
			{
				displayName: 'Use File Path for Video',
				name: 'videoUseFilePath',
				type: 'boolean',
				default: true,
				description: 'Use a file path for the input video. Uncheck to use binary data from a previous node.',
			},
			{
				displayName: 'Video File Path',
				name: 'videoFilePath',
				type: 'string',
				default: '',
				displayOptions: {
					show: {
						videoUseFilePath: [true],
					},
				},
				placeholder: '/path/to/video.mp4',
				description: 'The full file path to the input video file.',
				required: true,
			},
			{
				displayName: 'Video Binary Property Name',
				name: 'videoBinaryPropertyName',
				type: 'string',
				default: 'data',
				displayOptions: {
					show: {
						videoUseFilePath: [false],
					},
				},
				description: 'The name of the binary property containing the input video data.',
				required: true,
			},

			// ----------------------------------
			//      Inference Parameters
			// ----------------------------------
			{
				displayName: '--- Inference Parameters ---',
				name: 'inferenceParamsNotice',
				type: 'notice',
				default: 'Adjust the parameters for the video generation process.',
			},
			{
				displayName: 'BBox Shift (px)',
				name: 'bbox_shift',
				type: 'number',
				default: 0,
				description: 'The BBox_shift value in pixels.',
			},
			{
				displayName: 'Extra Margin',
				name: 'extra_margin',
				type: 'number',
				typeOptions: {
					minValue: 0,
					maxValue: 40,
				},
				default: 10,
				description: 'Determines the movement range of the jaw. Min: 0, Max: 40.',
			},
			{
				displayName: 'Parsing Mode',
				name: 'parsing_mode',
				type: 'options',
				options: [
					{
						name: 'Jaw',
						value: 'jaw',
					},
					{
						name: 'Raw',
						value: 'raw',
					},
				],
				default: 'jaw',
				description: 'The parsing mode to use for face analysis.',
			},
			{
				displayName: 'Left Cheek Width',
				name: 'left_cheek_width',
				type: 'number',
				typeOptions: {
					minValue: 20,
					maxValue: 160,
				},
				default: 90,
				description: "Determines the range of left cheek editing when parsing mode is 'jaw'. Min: 20, Max: 160.",
			},
			{
				displayName: 'Right Cheek Width',
				name: 'right_cheek_width',
				type: 'number',
				typeOptions: {
					minValue: 20,
					maxValue: 160,
				},
				default: 90,
				description: "Determines the range of right cheek editing when parsing mode is 'jaw'. Min: 20, Max: 160.",
			},
		],
	};

	async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
		const items = this.getInputData();
		const returnData: INodeExecutionData[] = [];

		for (let itemIndex = 0; itemIndex < items.length; itemIndex++) {
			try {
				// --- Get all node parameters ---
				const outputAsFilePath = this.getNodeParameter('outputAsFilePath', itemIndex, true) as boolean;
				const audioUseFilePath = this.getNodeParameter('audioUseFilePath', itemIndex, true) as boolean;
				const videoUseFilePath = this.getNodeParameter('videoUseFilePath', itemIndex, true) as boolean;

				// --- Handle Inputs (File Path vs Binary) ---
				let audioPath: string;
				if (audioUseFilePath) {
					audioPath = this.getNodeParameter('audioFilePath', itemIndex, '') as string;
				} else {
					const binaryPropertyName = this.getNodeParameter('audioBinaryPropertyName', itemIndex, 'data') as string;
					const binaryData = await this.helpers.getBinaryDataBuffer(itemIndex, binaryPropertyName);
					// Write binary data to a temporary file
					const tempPath = path.join(tmpdir(), `n8n-musetalk-audio-${Date.now()}`);
					await fs.writeFile(tempPath, binaryData);
					audioPath = tempPath;
				}

				let videoPath: string;
				if (videoUseFilePath) {
					videoPath = this.getNodeParameter('videoFilePath', itemIndex, '') as string;
				} else {
					const binaryPropertyName = this.getNodeParameter('videoBinaryPropertyName', itemIndex, 'data') as string;
					const binaryData = await this.helpers.getBinaryDataBuffer(itemIndex, binaryPropertyName);
					const tempPath = path.join(tmpdir(), `n8n-musetalk-video-${Date.now()}`);
					await fs.writeFile(tempPath, binaryData);
					videoPath = tempPath;
				}

				let outputFilePath: string;
				if (outputAsFilePath) {
					outputFilePath = this.getNodeParameter('outputFilePath', itemIndex, '') as string;
				} else {
					// If output is binary, we still need a temp path for the gatekeeper to write to.
					const outputBinaryPropertyName = this.getNodeParameter('outputBinaryPropertyName', itemIndex, 'data') as string;
					outputFilePath = path.join(tmpdir(), `n8n-musetalk-output-${Date.now()}-${outputBinaryPropertyName}.mp4`);
				}


				// --- Construct Gatekeeper Payload ---
				const gatekeeperPayload: IDataObject = {
					audio_path: audioPath,
					video_path: videoPath,
					output_file_path: outputFilePath,
					bbox_shift: this.getNodeParameter('bbox_shift', itemIndex, 0),
					extra_margin: this.getNodeParameter('extra_margin', itemIndex, 10),
					parsing_mode: this.getNodeParameter('parsing_mode', itemIndex, 'jaw'),
					left_cheek_width: this.getNodeParameter('left_cheek_width', itemIndex, 90),
					right_cheek_width: this.getNodeParameter('right_cheek_width', itemIndex, 90),
				};

				// --- Submit Job to Gatekeeper ---
				const initialOptions: IHttpRequestOptions = {
					method: 'POST',
					url: 'http://127.0.0.1:7861/execute', // Port for MuseTalk gatekeeper
					body: gatekeeperPayload,
					json: true,
				};

				const initialResponse = (await this.helpers.httpRequest(initialOptions)) as { job_id: string };
				const jobId = initialResponse.job_id;

				// --- Wait for Completion via WebSocket ---
				const finalResult = await new Promise<any>((resolve, reject) => {
					const ws = new WebSocket(`ws://127.0.0.1:7861/ws/${jobId}`);
					const timeout = setTimeout(() => {
						ws.close();
						reject(new NodeOperationError(this.getNode(), 'Job timed out. No response from Gatekeeper WebSocket.', { itemIndex }));
					}, 300000); // 5 minute timeout

					ws.on('message', (data: WebSocket.Data) => {
						clearTimeout(timeout);
						ws.close();
						try {
							resolve(JSON.parse(data.toString()));
						} catch (e) {
							reject(new NodeOperationError(this.getNode(), 'Failed to parse WebSocket message from Gatekeeper.', { itemIndex }));
						}
					});

					ws.on('error', (err: Error) => {
						clearTimeout(timeout);
						reject(new NodeOperationError(this.getNode(), `WebSocket connection error: ${err.message}`, { itemIndex }));
					});
				});

				// --- Process Final Result ---
				if (finalResult.format === 'error') {
					throw new NodeOperationError(this.getNode(), `Gatekeeper returned an error: ${finalResult.error}`, { itemIndex });
				}

				const output: INodeExecutionData = { json: {}, pairedItem: { item: itemIndex } };

				if (outputAsFilePath) {
					output.json.filePath = finalResult.data;
					output.json.filename = finalResult.filename;
				} else {
					const videoData = await fs.readFile(finalResult.data);
					const binaryData = await this.helpers.prepareBinaryData(videoData, finalResult.filename, 'video/mp4');
					const outputBinaryPropertyName = this.getNodeParameter('outputBinaryPropertyName', itemIndex, 'data') as string;
					output.binary = { [outputBinaryPropertyName]: binaryData };
					// Clean up the temporary output file
					await fs.unlink(finalResult.data);
				}

				// Clean up temporary input files if they were created
				if (!audioUseFilePath) await fs.unlink(audioPath);
				if (!videoUseFilePath) await fs.unlink(videoPath);

				returnData.push(output);

			} catch (error) {
				if (this.continueOnFail()) {
					returnData.push({ json: { error: error.message }, pairedItem: { item: itemIndex } });
					continue;
				}
				throw error;
			}
		}

		return [returnData];
	}
}
