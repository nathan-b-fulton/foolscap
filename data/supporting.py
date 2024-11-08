from collections import Counter

widths:dict = { 134: 77, 
                135: 69, 
                136: 78,
                137: 78, 
                138: 87, 
                139: 70, 
                140: 73, 
                141: 76, 
                142: 77, 
                143: 70, 
                144: 80, 
                145: 76,
                146: 82,
                147: 72,
                148: 83,
                149: 81,
                150: 79,
                151: 70,
                152: 84,
                153: 76,
                154: 80,
                155: 68,
                156: 80,
                157: 73, 
                158: 82,
                159: 73,
                160: 76,
                161: 70,
                162: 84,
                163: 74,
                164: 78,
                165: 70,
                166: 78,
                167: 72,
                168: 74,
                169: 70,
                170: 83,
                171: 72,
                172: 76,
                173: 68,
                174: 80,
                175: 74,
                176: 82,
                177: 75,
                178: 78,
                179: 74,
                180: 78,
                181: 73,
                182: 80,
                183: 71,
                184: 75,
                185: 73,
                186: 85,
                187: 72,
                188: 78,
                189: 75,
                190: 77,
                191: 69,
                192: 80,
                193: 75,
                194: 75,
                195: 75,
                196: 76,
                197: 74,
                198: 72,
                199: 71,
                200: 77,
                201: 72,
                202: 79,
                203: 73,
                204: 74,
                205: 75,
                206: 75,
                207: 71,
                208: 72,
                209: 81,
                210: 83,
                211: 75,
                212: 75,
                213: 75,
                214: 77,
                215: 75,
                216: 74,
                217: 74,
                218: 72,
                219: 74,
                220: 69,
                221: 71,
                222: 74,
                223: 74,
                224: 80,
                225: 71,
                226: 75,
                227: 74,
                228: 72,
                229: 73,
                230: 77,
                231: 68,
                232: 81,
                233: 71,
                234: 76,
                235: 76,
                236: 78,
                237: 78,
                238: 75,
                239: 71,
                240: 76,
                241: 81,
                242: 78,
                243: 75,
                244: 80,
                245: 78,
                246: 78,
                247: 67,
                248: 75,
                249: 67,
                250: 71,
                251: 73,
                252: 79,
                253: 71,
                254: 74,
                255: 70,
                256: 77,
                257: 69,
                258: 81,
                259: 65,
                260: 76,
                261: 71,
                262: 73,
                263: 72,
                264: 76,
                265: 76,
                266: 79,
                267: 83,
                268: 90,
                269: 76,
                270: 75,
                271: 72,
                272: 82,
                273: 79,
                274: 71,
                275: 71,
                276: 77,
                277: 93,
                278: 69,
                279: 73,
                280: 78,
                281: 70,
                282: 78,
                283: 71,
                284: 74,
                285: 69
                }

# print(Counter(widths.values()).most_common(2))
# [(75, 9), (78, 7)]